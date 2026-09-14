using System.Linq.Expressions;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;
using Polecat.Storage.FullText;
using Weasel.SqlServer;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses the full-text operators — <c>PlainTextSearch</c>, <c>PhraseSearch</c> and
///     <c>WebStyleSearch</c> — into <c>EXISTS</c> clauses against the token table gh-611 maintains
///     beside the document table.
/// </summary>
internal class FullTextSearchMethods: IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(LinqExtensions)
            && expression.Method.Name is "PlainTextSearch" or "PhraseSearch" or "WebStyleSearch";
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

        var searchTerm = WhereClauseParser.ExtractValue(expression.Arguments[1]) as string ?? string.Empty;

        var query = expression.Method.Name switch
        {
            "PhraseSearch" => FullTextQuery.Phrase(searchTerm),
            "WebStyleSearch" => FullTextQuery.WebStyle(searchTerm),
            _ => FullTextQuery.Plain(searchTerm)
        };

        return new FullTextFilter(mapping, index, query);
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
///     Renders a <see cref="FullTextQuery" /> as <c>EXISTS</c> clauses against the token table: OR
///     between groups, AND within one, and <c>NOT EXISTS</c> for an exclusion.
/// </summary>
internal class FullTextFilter: ISqlFragment
{
    private readonly DocumentMapping _mapping;
    private readonly FullTextIndex _index;
    private readonly FullTextQuery _query;

    public FullTextFilter(DocumentMapping mapping, FullTextIndex index, FullTextQuery query)
    {
        _mapping = mapping;
        _index = index;
        _query = query;
    }

    public void Apply(ICommandBuilder builder)
    {
        if (_query.IsEmpty)
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

        builder.Append("(");
        for (var g = 0; g < _query.OrGroups.Count; g++)
        {
            if (g > 0) builder.Append(" OR ");
            builder.Append("(");

            var group = _query.OrGroups[g];
            for (var c = 0; c < group.Count; c++)
            {
                if (c > 0) builder.Append(" AND ");
                AppendClause(builder, group[c], docTable, ftTable, conjoined);
            }

            builder.Append(")");
        }

        builder.Append(")");
    }

    private void AppendClause(ICommandBuilder builder, FullTextClause clause, string docTable, string ftTable,
        bool conjoined)
    {
        builder.Append(clause.Negated ? "NOT EXISTS (SELECT 1 FROM " : "EXISTS (SELECT 1 FROM ");
        builder.Append(ftTable);
        builder.Append(" f0");

        // A phrase is the terms in order and adjacent, which is what the stored position is for: join
        // the token table to itself once per following term, each pinned to pos + 1 of the one before.
        if (clause.Phrase)
        {
            for (var i = 1; i < clause.Terms.Length; i++)
            {
                builder.Append(" INNER JOIN ");
                builder.Append(ftTable);
                builder.Append($" f{i} ON f{i}.doc_id = f0.doc_id AND f{i}.member = f0.member AND f{i}.pos = f0.pos + {i}");
            }
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

        if (clause.Phrase)
        {
            for (var i = 0; i < clause.Terms.Length; i++)
            {
                builder.Append($" AND f{i}.term = ");
                builder.AppendParameter(clause.Terms[i]);
            }
        }
        else
        {
            // A non-phrase clause carries exactly one term; several bare words are several clauses,
            // so that each is independently required (or independently excluded).
            builder.Append(" AND f0.term = ");
            builder.AppendParameter(clause.Terms[0]);
        }

        builder.Append(")");
    }
}
