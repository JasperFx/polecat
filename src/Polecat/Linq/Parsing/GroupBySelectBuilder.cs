using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Turns a parsed GroupBy chain into its statement: GROUP BY columns, a select list, HAVING
///     clauses, the ordering and the terminal operator.
/// </summary>
/// <remarks>
///     <para>
///         <b>A key, an aggregate or a projected member all reach <see cref="TryResolve" />.</b> A
///         projection is written back in terms of the group it came from, so
///         <c>Select(g =&gt; new { C = g.Count() }).OrderBy(x =&gt; x.C)</c> orders by exactly the
///         expression <c>g.Count()</c> does, and the Select, the HAVING and the ORDER BY cannot disagree
///         about what a value is.
///     </para>
///     <para>
///         <b>A complex projection is a <see cref="JsonObjectFragment" />.</b> This used to concatenate
///         JSON by hand with <c>CONCAT</c>, which treated everything but a string as a number and
///         escaped only the double quote, so a Guid, DateTime or NULL key was invalid JSON and a
///         backslash in a key was reinterpreted (#837). The floor that sets is SQL Server 2022, which
///         is the oldest server Polecat tests against.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: evaluates HAVING-clause constants through WhereClauseParser.ExtractValue, which compiles anything it cannot read off a closure via Expression.Lambda — runtime code generation. Document types are preserved at the LINQ registration boundary per the AOT publishing guide.")]
internal class GroupBySelectBuilder
{
    /// <summary>The name a scalar group value goes by, because a derived table needs a named column.</summary>
    internal const string ScalarColumn = "__pc_v";

    private readonly MemberFactory _memberFactory;
    private readonly WhereClauseParser _predicates;

    // Key members for GROUP BY (maps anonymous type member name to its value)
    private readonly Dictionary<string, GroupedValue> _compositeKey = new();
    private GroupedValue? _simpleKey;
    private bool _isCompositeKey;

    // What the Select() projected, by member. Written against _grouping, so anything downstream of
    // the Select can be rewritten in terms of the group.
    private readonly Dictionary<string, Expression> _projectedMembers = new(StringComparer.OrdinalIgnoreCase);
    private Expression? _scalarProjection;
    private ParameterExpression? _grouping;

    public List<string> GroupByColumns { get; } = [];

    /// <summary>
    ///     Whether each row is a JSON object to deserialize rather than a scalar to read directly.
    /// </summary>
    public bool ReadsJson { get; private set; }

    public GroupBySelectBuilder(MemberFactory memberFactory)
    {
        _memberFactory = memberFactory;
        _predicates = new WhereClauseParser(memberFactory);
    }

    /// <summary>
    ///     Applies the parsed GroupBy chain to the parser's statement.
    /// </summary>
    public void ApplyTo(LinqQueryParser parser)
    {
        var statement = parser.Statement;

        ParseKeySelector(parser.GroupByKeySelector!);
        statement.GroupByColumns.AddRange(GroupByColumns);

        statement.SelectFragment = BuildSelect(parser);
        if (parser.IsDistinct) statement.IsDistinct = true;

        foreach (var predicate in parser.GroupByHavingPredicates)
        {
            var (body, grouping) = InTermsOfTheGroup(predicate);
            statement.HavingClauses.Add(ResolveHaving(body, grouping));
        }

        foreach (var (selector, descending) in parser.GroupByOrderings)
        {
            var (body, grouping) = InTermsOfTheGroup(selector);
            statement.OrderBys.Add(new OrderByClause(Resolve(body, grouping).Sql, descending, null));
        }

        // Pages of an unordered result can repeat or skip rows between requests. The key is unique
        // across groups, so ordering by it costs nothing and makes Skip/Take mean something.
        if (statement.Offset.HasValue && statement.OrderBys.Count == 0 && !statement.IsDistinct)
        {
            foreach (var column in GroupByColumns)
            {
                statement.OrderBys.Add((column, false));
            }
        }

        ApplyTerminalOperator(parser);
    }

    private void ParseKeySelector(LambdaExpression keySelector)
    {
        var body = LinqQueryParser.StripConvert(keySelector.Body);

        if (body is NewExpression newExpr)
        {
            _isCompositeKey = true;
            var parameters = newExpr.Constructor!.GetParameters();
            for (var i = 0; i < parameters.Length; i++)
            {
                var memberExpr = newExpr.Arguments[i] as MemberExpression
                    ?? throw new BadLinqExpressionException(
                        $"GroupBy composite key member must be a property, got: {newExpr.Arguments[i]}");
                var member = ClrOnlyMembers.AssertNoSubquery(_memberFactory.ResolveMember(memberExpr), "in a GroupBy key");
                _compositeKey[parameters[i].Name!] = KeyValue(member);
                GroupByColumns.Add(member.TypedLocator);
            }
        }
        else if (body is MemberExpression memberBody)
        {
            _isCompositeKey = false;
            var member = ClrOnlyMembers.AssertNoSubquery(_memberFactory.ResolveMember(memberBody), "in a GroupBy key");
            _simpleKey = KeyValue(member);
            GroupByColumns.Add(member.TypedLocator);
        }
        else
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate the GroupBy key '{body}' to SQL. Group by a document member "
                + "(x => x.Status) or by an anonymous type / constructor call over members "
                + "(x => new { x.Status, x.Color }) for a composite key.");
        }
    }

    private static GroupedValue KeyValue(IQueryableMember member)
    {
        var sql = new LiteralSqlFragment(member.TypedLocator);

        // A bool's locator yields the text 'true'. It stays that way for GROUP BY and for comparing
        // against the member's converted value, but in JSON it has to be the literal.
        ISqlFragment json = member.IsBoolean ? new LiteralSqlFragment($"CAST({member.TypedLocator} AS bit)") : sql;

        return new GroupedValue(sql, json, member.LocatorSqlType, member);
    }

    private ISqlFragment BuildSelect(LinqQueryParser parser)
    {
        var select = parser.SelectExpression;
        if (select == null)
        {
            // Counting or probing the groups needs nothing from them, so the Select is optional.
            if (parser.ValueMode is SingleValueMode.Count or SingleValueMode.LongCount or SingleValueMode.Any)
            {
                return new LiteralSqlFragment($"1 AS {ScalarColumn}");
            }

            throw new BadLinqExpressionException("GroupBy must be followed by a Select() projection.");
        }

        _grouping = select.Parameters[0];

        switch (SelectProjectionAnalyzer.StripSafeConvert(select.Body))
        {
            case NewExpression newExpr:
                ReadsJson = true;
                return AsJson(ProjectConstructor(newExpr));

            case MemberInitExpression memberInit:
                ReadsJson = true;
                var members = ProjectConstructor(memberInit.NewExpression);
                foreach (var binding in memberInit.Bindings.OfType<MemberAssignment>())
                {
                    members.Add(Project(binding.Member.Name, _memberFactory.FormatProjectedKey(binding.Member),
                        binding.Expression));
                }

                return AsJson(members);
        }

        var value = TryResolve(select.Body, _grouping)
            ?? throw new BadLinqExpressionException(
                $"Polecat cannot translate the GroupBy projection '{select.Body}' to SQL. Project into an "
                + "anonymous type, a constructor call, or an object initializer over g.Key and aggregates "
                + "(g => new { g.Key, Count = g.Count() }).");

        _scalarProjection = select.Body;
        return Scalar(value);
    }

    private static ISqlFragment Scalar(GroupedValue value)
        => new CompositeSqlFragment(value.Sql, $" AS {ScalarColumn}");

    private static ISqlFragment AsJson(List<JsonObjectMember> members)
        => new CompositeSqlFragment(new JsonObjectFragment(members), " AS data");

    private List<JsonObjectMember> ProjectConstructor(NewExpression newExpr)
    {
        var parameters = newExpr.Constructor!.GetParameters();
        var members = new List<JsonObjectMember>(parameters.Length);

        for (var i = 0; i < parameters.Length; i++)
        {
            // An anonymous type names its members; a positional constructor has only the parameter.
            var member = newExpr.Members?[i];
            members.Add(member != null
                ? Project(member.Name, _memberFactory.FormatProjectedKey(member), newExpr.Arguments[i])
                : Project(parameters[i].Name!, _memberFactory.FormatProjectedKey(parameters[i].Name!),
                    newExpr.Arguments[i]));
        }

        return members;
    }

    private JsonObjectMember Project(string name, string key, Expression argument)
    {
        var value = Resolve(argument, _grouping!);
        _projectedMembers[name] = argument;

        // The argument's own type rather than the value's: (int?)g.Max(...) says it can hold a null
        // even though what is inside the convert cannot.
        return new JsonObjectMember(key, value.Json, argument.Type, value.SqlType);
    }

    /// <summary>
    ///     The body of a lambda written downstream of a GroupBy, in terms of the group. Over the
    ///     grouping it already is. Over the Select() projection, its members are replaced by what was
    ///     assigned to them, so <c>x =&gt; x.Total</c> becomes <c>g.Sum(...)</c>.
    /// </summary>
    private (Expression Body, ParameterExpression Grouping) InTermsOfTheGroup(LambdaExpression lambda)
    {
        var parameter = lambda.Parameters[0];

        if (IsGrouping(parameter.Type)) return (lambda.Body, parameter);

        var rewritten = new ProjectionRewriter(parameter, _scalarProjection, _projectedMembers).Visit(lambda.Body);
        return (rewritten, _grouping!);
    }

    private static bool IsGrouping(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IGrouping<,>);

    private GroupedValue Resolve(Expression expression, ParameterExpression grouping)
    {
        return TryResolve(expression, grouping)
            ?? throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' inside a GroupBy projection. A projected value is g.Key "
                + "(or a member of a composite key), or an aggregate over the group — Count(), Sum(...), "
                + "Min(...), Max(...), Average().");
    }

    /// <summary>
    ///     The value of an expression over the group: <c>g.Key</c>, a member of a composite key, or an
    ///     aggregate. Null when it is none of those.
    /// </summary>
    private GroupedValue? TryResolve(Expression expression, ParameterExpression grouping)
    {
        var expr = SelectProjectionAnalyzer.StripSafeConvert(expression);

        // g.Key
        if (expr is MemberExpression memberAccess && memberAccess.Member.Name == "Key"
            && memberAccess.Expression == grouping)
        {
            if (_isCompositeKey)
            {
                throw new BadLinqExpressionException(
                    "Cannot select the entire composite GroupBy key directly. Access individual key members like g.Key.Color instead.");
            }

            return _simpleKey;
        }

        // g.Key.PropertyName (composite key)
        if (expr is MemberExpression compositeAccess
            && compositeAccess.Expression is MemberExpression innerMember
            && innerMember.Member.Name == "Key"
            && innerMember.Expression == grouping)
        {
            var propName = compositeAccess.Member.Name;

            if (!_isCompositeKey)
            {
                // A nullable key is stored as its value, so this is the key with its nulls unchanged.
                if (propName == nameof(Nullable<int>.Value) && Nullable.GetUnderlyingType(innerMember.Type) != null)
                {
                    return _simpleKey;
                }

                throw new BadLinqExpressionException(
                    $"Polecat cannot translate '{expr}' in a GroupBy: the key is one value, and SQL has nothing "
                    + $"to read {propName} from once the rows are grouped. Group by the member you want "
                    + "instead (x => x.CreatedAt.Year), or project g.Key and finish the work in memory.");
            }

            if (_compositeKey.TryGetValue(propName, out var info))
            {
                return info;
            }

            throw new BadLinqExpressionException(
                $"Unknown composite key member '{propName}' in GroupBy projection. The composite key's "
                + $"members are {string.Join(", ", _compositeKey.Keys)}.");
        }

        return expr is MethodCallExpression methodCall ? ResolveAggregate(methodCall, grouping) : null;
    }

    private GroupedValue? ResolveAggregate(MethodCallExpression method, ParameterExpression grouping)
    {
        var name = method.Method.Name;

        if (method.Arguments.Count == 0 || method.Arguments[0] != grouping) return null;

        // g.Count() / g.LongCount()
        if (name is "Count" or "LongCount" && method.Arguments.Count == 1)
        {
            return name == "LongCount" ? Aggregate("CAST(COUNT(*) AS bigint)") : Aggregate("COUNT(*)");
        }

        // g.Count(predicate) / g.LongCount(predicate). T-SQL has no FILTER clause, and COUNT skips the
        // NULL an unmatched CASE yields. The predicate is a fragment rather than text because it binds
        // its values as parameters, like any other predicate.
        if (name is "Count" or "LongCount" && method.Arguments.Count == 2)
        {
            var predicate = _predicates.Parse(LinqQueryParser.GetLambda(method.Arguments[1]).Body);
            AssertNoSubquery(predicate, method);

            var count = new CompositeSqlFragment("COUNT(CASE WHEN ", predicate, " THEN 1 END)");
            return name == "LongCount"
                ? new GroupedValue(new CompositeSqlFragment("CAST(", count, " AS bigint)"))
                : new GroupedValue(count);
        }

        // g.Sum(x => x.Prop) / g.Min() / g.Max() / g.Average()
        if (name is "Sum" or "Min" or "Max" or "Average" && method.Arguments.Count == 2)
        {
            var lambda = LinqQueryParser.GetLambda(method.Arguments[1]);
            var body = LinqQueryParser.StripConvert(lambda.Body);
            if (body is MemberExpression memberExpr)
            {
                var member = ClrOnlyMembers.AssertNoSubquery(_memberFactory.ResolveMember(memberExpr), $"inside {name}()");
                return name switch
                {
                    "Sum" => Aggregate($"ISNULL(SUM({member.TypedLocator}), 0)"),
                    "Min" => Aggregate($"MIN({member.TypedLocator})", member.LocatorSqlType),
                    "Max" => Aggregate($"MAX({member.TypedLocator})", member.LocatorSqlType),
                    "Average" => Aggregate($"AVG(CAST({member.TypedLocator} AS float))"),
                    _ => null
                };
            }
        }

        return null;
    }

    private static GroupedValue Aggregate(string sql, string? sqlType = null)
        => new(new LiteralSqlFragment(sql), SqlType: sqlType);

    /// <summary>
    ///     SQL Server refuses an aggregate over a subquery (Msg 130), and a predicate over a child
    ///     collection or a long value list is one. The refusal here says what to do about it.
    /// </summary>
    private static void AssertNoSubquery(ISqlFragment predicate, MethodCallExpression method)
    {
        var builder = new BatchBuilder();
        predicate.Apply(builder);

        if (builder.Compile().BatchCommands[0].CommandText.Contains("(SELECT ", StringComparison.OrdinalIgnoreCase))
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{method}' because its predicate needs a subquery, and SQL Server "
                + "cannot aggregate over one. Filter with a Where in front of the GroupBy, or use a predicate "
                + "over scalar members.");
        }
    }

    /// <summary>
    ///     Resolves a HAVING expression (from Where() after GroupBy()) to an ISqlFragment.
    /// </summary>
    public ISqlFragment ResolveHaving(Expression expression, ParameterExpression groupingParam)
    {
        if (WhereClauseParser.TryEvaluate(expression, out var known))
        {
            return new WhereFragment(known ? "1=1" : "1=0");
        }

        if (expression is BinaryExpression binary)
        {
            return ResolveHavingBinary(binary, groupingParam);
        }

        throw new BadLinqExpressionException(
            $"Polecat cannot translate '{expression.NodeType}' into a HAVING clause. A post-GroupBy Where "
            + "compares an aggregate or the key against a value, e.g. g => g.Count() > 5.");
    }

    private ISqlFragment ResolveHavingBinary(BinaryExpression binary, ParameterExpression groupingParam)
    {
        if (WhereClauseParser.TryShortCircuit(binary) is { } reduced) return ResolveHaving(reduced, groupingParam);

        if (binary.NodeType == ExpressionType.AndAlso)
        {
            var left = ResolveHaving(binary.Left, groupingParam);
            var right = ResolveHaving(binary.Right, groupingParam);
            return new CompoundHavingFragment("AND", left, right);
        }

        if (binary.NodeType == ExpressionType.OrElse)
        {
            var left = ResolveHaving(binary.Left, groupingParam);
            var right = ResolveHaving(binary.Right, groupingParam);
            return new CompoundHavingFragment("OR", left, right);
        }

        var op = binary.NodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new BadLinqExpressionException(
                $"Polecat cannot translate the comparison '{binary.NodeType}' into a HAVING clause. "
                + "Supported operators are ==, !=, <, <=, > and >=.")
        };

        var leftOperand = ResolveHavingOperand(binary.Left, groupingParam);
        var rightOperand = ResolveHavingOperand(binary.Right, groupingParam);

        // x = NULL is never true in SQL, so a comparison against a null has to be spelled IS NULL.
        if (op is "=" or "!=")
        {
            var nullCheck = op == "=" ? " IS NULL" : " IS NOT NULL";
            if (rightOperand is { IsConstant: true, Value: null })
            {
                return new CompositeSqlFragment(leftOperand.Fragment!, nullCheck);
            }

            if (leftOperand is { IsConstant: true, Value: null })
            {
                return new CompositeSqlFragment(rightOperand.Fragment!, nullCheck);
            }
        }

        return new HavingComparisonFragment(
            leftOperand.ComparedTo(rightOperand), op, rightOperand.ComparedTo(leftOperand));
    }

    /// <summary>
    ///     An operand of a HAVING comparison: either a server-side expression over the group (safe,
    ///     built from developer-controlled member locators) or a runtime constant value. Constants
    ///     are bound as command parameters rather than rendered into the SQL text — the constant is a
    ///     captured local / request value and rendering its <c>ToString()</c> verbatim was a SQL
    ///     injection vector (CWE-89), the same class as JasperFx/marten#4954 (non-string projection
    ///     constants rendered unparameterized).
    /// </summary>
    private HavingOperand ResolveHavingOperand(Expression expr, ParameterExpression groupingParam)
    {
        var value = TryResolve(expr, groupingParam);
        if (value != null) return HavingOperand.Aggregate(value.Sql, value.Member);

        try
        {
            return HavingOperand.Constant(WhereClauseParser.ExtractValue(expr));
        }
        catch
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate the HAVING operand '{expr}' to SQL. An operand is an aggregate "
                + "over the group, the group key, or a constant.");
        }
    }

    private void ApplyTerminalOperator(LinqQueryParser parser)
    {
        var statement = parser.Statement;

        switch (parser.ValueMode)
        {
            case SingleValueMode.First:
            case SingleValueMode.FirstOrDefault:
                statement.Limit = 1;
                break;

            case SingleValueMode.Single:
            case SingleValueMode.SingleOrDefault:
                // Fetch 2 rows to detect "more than one element"
                statement.Limit = 2;
                break;

            case SingleValueMode.Last:
            case SingleValueMode.LastOrDefault:
                for (var i = 0; i < statement.OrderBys.Count; i++)
                {
                    statement.OrderBys[i] = statement.OrderBys[i].Reversed();
                }

                statement.Limit = 1;
                break;

            case SingleValueMode.Any:
                statement.Limit = 1;
                statement.IsExistsWrapper = true;
                break;

            case SingleValueMode.Count:
                OverTheGroups(statement, "COUNT(*)");
                break;

            case SingleValueMode.LongCount:
                OverTheGroups(statement, "CAST(COUNT(*) AS bigint)");
                break;

            case SingleValueMode.Sum:
                AggregateTheGroups(parser, $"ISNULL(SUM({ScalarColumn}), 0)");
                break;

            case SingleValueMode.Min:
                AggregateTheGroups(parser, $"MIN({ScalarColumn})");
                break;

            case SingleValueMode.Max:
                AggregateTheGroups(parser, $"MAX({ScalarColumn})");
                break;

            case SingleValueMode.Average:
                AggregateTheGroups(parser, $"AVG(CAST({ScalarColumn} AS float))");
                break;
        }
    }

    private void AggregateTheGroups(LinqQueryParser parser, string aggregate)
    {
        GroupedValue value;
        if (parser.GroupByAggregateSelector is { } selector)
        {
            var (body, grouping) = InTermsOfTheGroup(selector);
            value = Resolve(body, grouping);
        }
        else
        {
            value = Resolve(_scalarProjection!, _grouping!);
        }

        parser.Statement.SelectFragment = Scalar(value);
        OverTheGroups(parser.Statement, aggregate);
    }

    private static void OverTheGroups(Statement statement, string aggregate)
    {
        // A derived table may only carry an ORDER BY that decides which rows it holds, which is to
        // say when it is paged. Anywhere else it changes nothing about a count and SQL Server refuses it.
        if (!statement.Limit.HasValue && !statement.Offset.HasValue)
        {
            statement.OrderBys.Clear();
        }

        statement.OuterAggregate = aggregate;
    }

    /// <summary>
    ///     A value over the group. <see cref="Json" /> is what it is written into a JSON object as, which
    ///     differs from <paramref name="Sql" /> only for a bool key.
    /// </summary>
    private sealed record GroupedValue(
        ISqlFragment Sql, ISqlFragment? JsonOverride = null, string? SqlType = null, IQueryableMember? Member = null)
    {
        public ISqlFragment Json => JsonOverride ?? Sql;
    }

    /// <summary>
    ///     Rewrites a lambda over the Select() projection into the expressions that were projected.
    /// </summary>
    private sealed class ProjectionRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter;
        private readonly Expression? _scalar;
        private readonly Dictionary<string, Expression> _members;

        public ProjectionRewriter(ParameterExpression parameter, Expression? scalar,
            Dictionary<string, Expression> members)
        {
            _parameter = parameter;
            _scalar = scalar;
            _members = members;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != _parameter) return base.VisitMember(node);

            return _members.TryGetValue(node.Member.Name, out var projected)
                ? projected
                : throw new BadLinqExpressionException(
                    $"'{node.Member.Name}' is not a member the GroupBy's Select(...) projected, so Polecat "
                    + "has no SQL for it.");
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => node == _parameter && _scalar != null ? _scalar : base.VisitParameter(node);
    }
}

/// <summary>
///     One side of a HAVING comparison — either SQL rendered inline (developer-controlled, though it
///     may bind parameters of its own) or a runtime constant value that must be bound as a parameter.
/// </summary>
internal readonly struct HavingOperand
{
    private HavingOperand(ISqlFragment? fragment, object? value, bool isConstant, IQueryableMember? member)
    {
        Fragment = fragment;
        Value = value;
        IsConstant = isConstant;
        Member = member;
    }

    public ISqlFragment? Fragment { get; }
    public object? Value { get; }
    public bool IsConstant { get; }

    /// <summary>The group key member behind this operand, when it is one.</summary>
    public IQueryableMember? Member { get; }

    public static HavingOperand Aggregate(string sql) => new(new LiteralSqlFragment(sql), null, false, null);

    public static HavingOperand Aggregate(ISqlFragment sql, IQueryableMember? member = null)
        => new(sql, null, false, member);

    public static HavingOperand Constant(object? value) => new(null, value, true, null);

    /// <summary>
    ///     This operand as it should be bound opposite <paramref name="other" />. A constant takes on
    ///     the stored form of the key it is compared to: an enum stored as a name, a bool stored as text.
    /// </summary>
    public HavingOperand ComparedTo(HavingOperand other)
        => IsConstant && other.Member != null ? Constant(other.Member.ConvertValue(Value)) : this;
}

/// <summary>
///     Emits <c>{left} {op} {right}</c> for a HAVING comparison. Aggregate operands are appended as
///     SQL text; constant operands are bound as command parameters so an attacker-influenced value
///     cannot break out of the SQL grammar.
/// </summary>
internal class HavingComparisonFragment : ISqlFragment
{
    private readonly HavingOperand _left;
    private readonly string _op;
    private readonly HavingOperand _right;

    public HavingComparisonFragment(HavingOperand left, string op, HavingOperand right)
    {
        _left = left;
        _op = op;
        _right = right;
    }

    public void Apply(ICommandBuilder builder)
    {
        ApplyOperand(builder, _left);
        builder.Append(" ");
        builder.Append(_op);
        builder.Append(" ");
        ApplyOperand(builder, _right);
    }

    private static void ApplyOperand(ICommandBuilder builder, HavingOperand operand)
    {
        if (!operand.IsConstant)
        {
            operand.Fragment!.Apply(builder);
            return;
        }

        if (operand.Value == null)
        {
            builder.Append("NULL");
            return;
        }

        ComparisonFilter.AppendValue(builder, operand.Value);
    }
}

internal class CompoundHavingFragment : ISqlFragment
{
    private readonly string _separator;
    private readonly ISqlFragment _left;
    private readonly ISqlFragment _right;

    public CompoundHavingFragment(string separator, ISqlFragment left, ISqlFragment right)
    {
        _separator = separator;
        _left = left;
        _right = right;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append("(");
        _left.Apply(builder);
        builder.Append($" {_separator} ");
        _right.Apply(builder);
        builder.Append(")");
    }
}
