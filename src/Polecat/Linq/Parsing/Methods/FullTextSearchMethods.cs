using System.Linq.Expressions;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;
using Polecat.Storage.FullText;
using Weasel.SqlServer;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses the full-text operators — <c>PlainTextSearch</c> and <c>PhraseSearch</c> — into an
///     <c>EXISTS</c> against the token table gh-611 maintains beside the document table.
/// </summary>
internal class FullTextSearchMethods: IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(LinqExtensions)
            && expression.Method.Name is "PlainTextSearch" or "PhraseSearch";
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        // The mapping, not just the member: the token table's name and the tenancy that keys it both
        // come from there, and IMemberResolver carries neither.
        if (memberFactory is not MemberFactory factory)
        {
            throw new BadLinqExpressionException(
                $"{expression.Method.Name}() is only supported against a document query.");
        }

        var stripped = StripConvert(expression.Arguments[0]);
        if (stripped is not MemberExpression member)
        {
            throw new BadLinqExpressionException(
                $"{expression.Method.Name}() has to be applied to a member of the document, "
                + $"got: {stripped}.");
        }

        var mapping = factory.Mapping;
        var memberName = MemberNameOf(member);

        var index = mapping.FullTextIndexes.FirstOrDefault(x => x.MemberName == memberName)
                    ?? throw new BadLinqExpressionException(
                        mapping.FullTextIndexes.Count == 0
                            ? $"'{mapping.DocumentType.Name}' declares no full-text index, so there is "
                              + $"nothing to search. Declare one with "
                              + $"Schema.For<{mapping.DocumentType.Name}>().FullTextIndex(x => x.{memberName})."
                            : $"'{mapping.DocumentType.Name}.{memberName}' is not a declared full-text "
                              + "member. Declared: "
                              + string.Join(", ", mapping.FullTextIndexes.Select(x => x.MemberName)) + ".");

        var searchTerm = WhereClauseParser.ExtractValue(expression.Arguments[1]) as string;
        var terms = FullTextIndex.Tokenize(searchTerm ?? string.Empty);

        return new FullTextFilter(mapping, index, terms, expression.Method.Name == "PhraseSearch");
    }

    private static string MemberNameOf(MemberExpression expression)
    {
        var parts = new List<string>();
        Expression? current = expression;
        while (current is MemberExpression me)
        {
            parts.Insert(0, me.Member.Name);
            current = me.Expression;
        }

        return string.Join(".", parts);
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expression = unary.Operand;
        return expression;
    }
}

/// <summary>
///     <c>EXISTS</c> against the token table. One per term for a plain search — every term has to be
///     present — or one self-joined chain on consecutive positions for a phrase.
/// </summary>
internal class FullTextFilter: ISqlFragment
{
    private readonly DocumentMapping _mapping;
    private readonly FullTextIndex _index;
    private readonly string[] _terms;
    private readonly bool _phrase;

    public FullTextFilter(DocumentMapping mapping, FullTextIndex index, string[] terms, bool phrase)
    {
        _mapping = mapping;
        _index = index;
        _terms = terms;
        _phrase = phrase;
    }

    public void Apply(ICommandBuilder builder)
    {
        if (_terms.Length == 0)
        {
            // An empty search matches nothing rather than everything. Marten's plainto_tsquery('')
            // behaves the same way, and the alternative — a search box the user has not typed into
            // returning the entire table — is the worse surprise.
            builder.Append("1=0");
            return;
        }

        var docTable = SqlEscaping.QualifiedName(_mapping.DatabaseSchemaName, _mapping.TableName);
        var ftTable = SqlEscaping.QualifiedName(_mapping.DatabaseSchemaName, FullTextIndex.TableNameFor(_mapping));
        var conjoined = _mapping.TenancyStyle == TenancyStyle.Conjoined;

        if (_phrase)
        {
            AppendPhrase(builder, docTable, ftTable, conjoined);
            return;
        }

        builder.Append("(");
        for (var i = 0; i < _terms.Length; i++)
        {
            if (i > 0) builder.Append(" AND ");
            builder.Append("EXISTS (SELECT 1 FROM ");
            builder.Append(ftTable);
            builder.Append(" ft WHERE ft.doc_id = ");
            builder.Append(docTable);
            builder.Append(".id");
            if (conjoined)
            {
                builder.Append(" AND ft.tenant_id = ");
                builder.Append(docTable);
                builder.Append(".tenant_id");
            }

            builder.Append(" AND ft.member = ");
            builder.AppendParameter(_index.MemberName);
            builder.Append(" AND ft.term = ");
            builder.AppendParameter(_terms[i]);
            builder.Append(")");
        }

        builder.Append(")");
    }

    /// <summary>
    ///     A phrase is the terms in order and adjacent, which is what the stored position is for: join
    ///     the token table to itself once per following term, each pinned to <c>pos + 1</c> of the one
    ///     before it.
    /// </summary>
    private void AppendPhrase(ICommandBuilder builder, string docTable, string ftTable, bool conjoined)
    {
        builder.Append("EXISTS (SELECT 1 FROM ");
        builder.Append(ftTable);
        builder.Append(" f0");

        for (var i = 1; i < _terms.Length; i++)
        {
            builder.Append(" INNER JOIN ");
            builder.Append(ftTable);
            builder.Append($" f{i} ON f{i}.doc_id = f0.doc_id AND f{i}.member = f0.member AND f{i}.pos = f0.pos + {i}");
        }

        builder.Append(" WHERE f0.doc_id = ");
        builder.Append(docTable);
        builder.Append(".id");
        if (conjoined)
        {
            builder.Append(" AND f0.tenant_id = ");
            builder.Append(docTable);
            builder.Append(".tenant_id");
        }

        builder.Append(" AND f0.member = ");
        builder.AppendParameter(_index.MemberName);

        for (var i = 0; i < _terms.Length; i++)
        {
            builder.Append($" AND f{i}.term = ");
            builder.AppendParameter(_terms[i]);
        }

        builder.Append(")");
    }
}
