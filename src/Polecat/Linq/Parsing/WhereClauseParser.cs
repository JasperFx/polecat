using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing.Methods;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Parses a predicate expression tree into an ISqlFragment for the WHERE clause.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: evaluates constant sub-expressions in predicates via Expression.Lambda + Compile — runtime code generation. Document/event types and their property graph are preserved at the LINQ registration boundary on the caller side per the AOT publishing guide.")]
internal class WhereClauseParser
{
    private static readonly Dictionary<ExpressionType, string> Operators = new()
    {
        { ExpressionType.Equal, "=" },
        { ExpressionType.NotEqual, "!=" },
        { ExpressionType.GreaterThan, ">" },
        { ExpressionType.GreaterThanOrEqual, ">=" },
        { ExpressionType.LessThan, "<" },
        { ExpressionType.LessThanOrEqual, "<=" }
    };

    private readonly IMemberResolver _memberFactory;
    private readonly IReadOnlyList<IMethodCallParser>? _additionalParsers;

    // #835: how many NOTs enclose the expression being parsed. A comparison that reads UNKNOWN drops its
    // row either way, which is only the right answer when nothing above it negates the comparison, and
    // that NOT can be any number of levels up. See QueryableMemberExtensions.ComparisonLocator.
    private int _negations;

    private bool IsPositive => _negations == 0;

    public WhereClauseParser(IMemberResolver memberFactory,
        IReadOnlyList<IMethodCallParser>? additionalParsers = null)
    {
        _memberFactory = memberFactory;
        _additionalParsers = additionalParsers;
    }

    public ISqlFragment Parse(Expression expression) => ParsePredicate(ClrOnlyMembers.Normalize(expression));

    private ISqlFragment ParsePredicate(Expression expression)
    {
        // #840: no document in it, so it is decided here and SQL only hears the answer.
        // !captured.HasValue || x.Age != captured was refused, and captured == null too.
        if (TryEvaluate(expression, out var known))
        {
            return known ? new WhereFragment("1=1") : new WhereFragment("1=0");
        }

        return expression switch
        {
            BinaryExpression binary => ParseBinary(binary),
            UnaryExpression { NodeType: ExpressionType.Not } unary => ParseNot(unary),
            UnaryExpression { NodeType: ExpressionType.Convert } unary => ParsePredicate(unary.Operand),
            MethodCallExpression methodCall => ParseMethodCall(methodCall),
            MemberExpression member when IsBooleanMember(member) => ParseBooleanMember(member, true),
            _ => throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to a SQL WHERE clause ({expression.NodeType}, "
                + $"{expression.GetType().Name}). A predicate is built from member comparisons "
                + "(x.Name == \"Bob\"), && and || over those, !, a boolean member, and the supported method "
                + $"calls ({MethodCallParserRegistry.SupportedCalls}). To go outside that, run the "
                + "query as raw SQL through session.AdvancedSql.QueryAsync<T>(...), or materialize it and "
                + "filter the results in memory.")
        };
    }

    private ISqlFragment ParseMethodCall(MethodCallExpression expression)
    {
        if (_additionalParsers != null)
        {
            foreach (var additional in _additionalParsers)
            {
                if (additional.Matches(expression))
                    return additional.Parse(_memberFactory, expression, IsPositive);
            }
        }

        var parser = MethodCallParserRegistry.FindParser(expression)
            ?? throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression.Method.DeclaringType?.Name}.{expression.Method.Name}' "
                + $"to SQL in a WHERE clause. Supported calls are {MethodCallParserRegistry.SupportedCalls}. "
                + "Otherwise run the query as raw SQL through session.AdvancedSql.QueryAsync<T>(...), or "
                + "materialize it and filter in memory.");

        return parser.Parse(_memberFactory, expression, IsPositive);
    }

    private ISqlFragment ParseBinary(BinaryExpression binary)
    {
        if (TryShortCircuit(binary) is { } reduced) return ParsePredicate(reduced);

        // Logical operators: AND, OR
        if (binary.NodeType == ExpressionType.AndAlso)
            return new CompoundWhereFragment("AND", ParsePredicate(binary.Left), ParsePredicate(binary.Right));

        if (binary.NodeType == ExpressionType.OrElse)
            return new CompoundWhereFragment("OR", ParsePredicate(binary.Left), ParsePredicate(binary.Right));

        // Comparison operators: ==, !=, <, >, <=, >=
        if (Operators.TryGetValue(binary.NodeType, out var op))
        {
            return ParseComparison(binary, op);
        }

        throw new BadLinqExpressionException(
            $"Polecat cannot translate the binary operator '{binary.NodeType}' to SQL, in '{binary}'. "
            + "Supported operators are ==, !=, <, <=, >, >=, && and ||. For anything else run the query "
            + "as raw SQL through session.AdvancedSql.QueryAsync<T>(...), or materialize it and filter in "
            + "memory.");
    }

    private ISqlFragment ParseComparison(BinaryExpression binary, string op)
    {
        // Handle modulo: x.Number % 2 == 0 → (member % divisor) op value
        if (TryParseModulo(binary, op, out var moduloFragment))
        {
            return moduloFragment!;
        }

        // jasperfx#740: member arithmetic compared to a value — x.Version - x.CompactedVersion > N,
        // the Stream Compaction Policy's growth predicate. Both orientations.
        if (TryParseMemberArithmetic(binary.Left, binary.Right, op, out var arithmeticFragment))
        {
            return arithmeticFragment!;
        }

        if (TryParseMemberArithmetic(binary.Right, binary.Left, ReverseOperator(op), out arithmeticFragment))
        {
            return arithmeticFragment!;
        }

        // Handle string transform methods: x.Name.ToLower() == "value"
        if (TryParseMethodTransform(binary.Left, binary.Right, op, out var transformFragment))
        {
            return transformFragment!;
        }

        if (TryParseMethodTransform(binary.Right, binary.Left, ReverseOperator(op), out transformFragment))
        {
            return transformFragment!;
        }

        // Try to resolve: left=member, right=value
        if (TryResolveMemberAndValue(binary.Left, binary.Right, out var member, out var value))
        {
            return BuildComparisonFilter(member!, value, op);
        }

        // Try reversed: left=value, right=member
        if (TryResolveMemberAndValue(binary.Right, binary.Left, out member, out value))
        {
            // Reverse the operator for reversed operands
            op = ReverseOperator(op);
            return BuildComparisonFilter(member!, value, op);
        }

        throw new BadLinqExpressionException(
            $"Polecat cannot translate the comparison '{binary}' to SQL: neither side resolves to a "
            + "document member compared against a constant. Compare a member to a value "
            + "(x.Age > 30), or run the query as raw SQL through "
            + "session.AdvancedSql.QueryAsync<T>(...).");
    }

    private bool TryParseMethodTransform(Expression methodSide, Expression valueSide, string op,
        out ISqlFragment? fragment)
    {
        fragment = null;

        var unwrapped = StripConvert(methodSide);
        if (unwrapped is not MethodCallExpression methodCall) return false;

        var parser = MethodCallParserRegistry.FindParser(methodCall);
        if (parser == null) return false;

        var result = parser.Parse(_memberFactory, methodCall);
        if (result is not SqlFunctionLocator funcLocator) return false;

        var value = ExtractValue(valueSide);
        if (value == null)
        {
            fragment = op == "="
                ? new WhereFragment($"{funcLocator.FullLocator} IS NULL")
                : new WhereFragment($"{funcLocator.FullLocator} IS NOT NULL");
        }
        else
        {
            fragment = new ComparisonFilter(funcLocator.FullLocator, op, value);
        }

        return true;
    }

    /// <summary>
    ///     jasperfx#740: (member + member) / (member - member) compared against a value —
    ///     e.g. <c>x.Version - x.CompactedVersion &gt; N</c>, the compaction policy's un-compacted
    ///     growth predicate. Restricted to two document members so the emitted SQL is pure column
    ///     arithmetic; the comparison value rides a parameter like any other comparison.
    /// </summary>
    private bool TryParseMemberArithmetic(Expression arithmeticSide, Expression valueSide, string op,
        out ISqlFragment? fragment)
    {
        fragment = null;

        if (StripConvert(arithmeticSide) is not BinaryExpression arithmetic) return false;

        var sqlOperator = arithmetic.NodeType switch
        {
            ExpressionType.Subtract or ExpressionType.SubtractChecked => "-",
            ExpressionType.Add or ExpressionType.AddChecked => "+",
            _ => null
        };
        if (sqlOperator == null) return false;

        if (StripConvert(arithmetic.Left) is not MemberExpression leftMember
            || !IsDocumentMember(leftMember)) return false;
        if (StripConvert(arithmetic.Right) is not MemberExpression rightMember
            || !IsDocumentMember(rightMember)) return false;

        var left = _memberFactory.ResolveMember(leftMember);
        var right = _memberFactory.ResolveMember(rightMember);
        var value = ExtractValue(valueSide);
        if (value == null) return false;

        fragment = new ComparisonFilter($"({left.TypedLocator} {sqlOperator} {right.TypedLocator})", op, value);
        return true;
    }

    private bool TryParseModulo(BinaryExpression binary, string op, out ISqlFragment? fragment)
    {
        fragment = null;

        // Pattern: (x.Number % divisor) op value
        if (binary.Left is BinaryExpression { NodeType: ExpressionType.Modulo } modulo)
        {
            var unwrapped = StripConvert(modulo.Left);
            if (unwrapped is MemberExpression me && IsDocumentMember(me))
            {
                var member = _memberFactory.ResolveMember(me);
                var divisor = ExtractValue(modulo.Right);
                var value = ExtractValue(binary.Right);

                // These two are written into the SQL rather than bound, so they have to be SQL's
                // numbers and not the culture's (#848): de-DE would write 2,5 and sv-SE U+2212 for -1.
                fragment = new WhereFragment(string.Create(CultureInfo.InvariantCulture,
                    $"({member.TypedLocator} % {divisor}) {op} {value}"));
                return true;
            }
        }

        // Reversed pattern: value op (x.Number % divisor)
        if (binary.Right is BinaryExpression { NodeType: ExpressionType.Modulo } moduloR)
        {
            var unwrapped = StripConvert(moduloR.Left);
            if (unwrapped is MemberExpression me && IsDocumentMember(me))
            {
                var member = _memberFactory.ResolveMember(me);
                var divisor = ExtractValue(moduloR.Right);
                var value = ExtractValue(binary.Left);
                var reversedOp = ReverseOperator(op);
                fragment = new WhereFragment(string.Create(CultureInfo.InvariantCulture,
                    $"({member.TypedLocator} % {divisor}) {reversedOp} {value}"));
                return true;
            }
        }

        return false;
    }

    private ISqlFragment BuildComparisonFilter(IQueryableMember member, object? value, string op)
    {
        // x.N.HasValue == false is IS NULL, like !x.N.HasValue, rather than a comparison over a CASE.
        if (member is HasValueMember hasValue && value is bool expected)
        {
            return hasValue.Is(op == "=" ? expected : !expected);
        }

        // Handle null comparisons
        if (value == null)
        {
            return op == "="
                ? new WhereFragment($"{member.RawLocator} IS NULL")
                : new WhereFragment($"{member.RawLocator} IS NOT NULL");
        }

        var convertedValue = member.ConvertValue(value);
        return new ComparisonFilter(member.ComparisonLocator(op, IsPositive, value), op, convertedValue!);
    }

    private ISqlFragment ParseNot(UnaryExpression unary)
    {
        var operand = unary.Operand;

        // !x.IsActive
        if (operand is MemberExpression member && IsBooleanMember(member))
        {
            return ParseBooleanMember(member, false);
        }

        // !(expression) — negate the inner expression
        _negations++;
        try
        {
            return new NotFragment(ParsePredicate(operand));
        }
        finally
        {
            _negations--;
        }
    }

    private ISqlFragment ParseBooleanMember(MemberExpression memberExpr, bool expectedValue)
    {
        var resolvedMember = _memberFactory.ResolveMember(memberExpr);

        // Handle Nullable<T>.HasValue → IS NULL / IS NOT NULL
        if (resolvedMember is HasValueMember hasValue)
        {
            return hasValue.Is(expectedValue);
        }

        var value = resolvedMember.ConvertValue(expectedValue);
        return new ComparisonFilter(resolvedMember.TypedLocator, "=", value!);
    }

    private bool TryResolveMemberAndValue(Expression memberExpr, Expression valueExpr,
        out IQueryableMember? member, out object? value)
    {
        member = null;
        value = null;

        var unwrappedMember = StripConvert(memberExpr);

        if (unwrappedMember is MemberExpression me && IsDocumentMember(me))
        {
            member = _memberFactory.ResolveMember(me);
            value = ExtractValue(valueExpr);
            return true;
        }

        return false;
    }

    internal static object? ExtractValue(Expression expression)
    {
        expression = StripConvert(expression);

        // Direct constant
        if (expression is ConstantExpression constant)
            return constant.Value;

        // Closure variable: field on a captured closure object. A Nullable<T> constant is left to the
        // compiled path: it boxes to null or to a bare T, and neither is a target for its own getters.
        if (expression is MemberExpression { Expression: ConstantExpression closureObj } memberExpr
            && Nullable.GetUnderlyingType(closureObj.Type) == null)
        {
            return memberExpr.Member switch
            {
                FieldInfo field => field.GetValue(closureObj.Value),
                PropertyInfo prop => prop.GetValue(closureObj.Value),
                _ => CompileAndInvoke(expression)
            };
        }

        // Nested member access (e.g., obj.Property.Field)
        if (expression is MemberExpression nestedMember)
        {
            return CompileAndInvoke(expression);
        }

        // Array or other complex expression — compile and evaluate
        return CompileAndInvoke(expression);
    }

    private static object? CompileAndInvoke(Expression expression)
    {
        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();

        try
        {
            return compiled.DynamicInvoke();
        }
        catch (TargetInvocationException e) when (e.InnerException != null)
        {
            // An empty captured Nullable's .Value is the usual one.
            throw new BadLinqExpressionException(
                $"Polecat could not evaluate '{expression}' while translating the query: "
                + e.InnerException.Message, e.InnerException);
        }
    }

    /// <summary>
    ///     The value of a boolean expression with no query parameter in it, which can be worked out now
    ///     rather than translated. <c>captured.HasValue</c> is the usual one.
    /// </summary>
    internal static bool TryEvaluate(Expression expression, out bool value)
    {
        value = false;

        if (expression.Type != typeof(bool) || ParameterFinder.ReferencesParameter(expression)) return false;

        value = (bool)ExtractValue(expression)!;
        return true;
    }

    /// <summary>
    ///     What is left of <c>left &amp;&amp; right</c> or <c>left || right</c> when <c>left</c> can be
    ///     evaluated now, or null when it cannot. The right side of a decided operator is not touched:
    ///     in <c>!captured.HasValue || x.Age != captured.Value</c> it cannot even be evaluated when the
    ///     left side has already answered, which is the reason the guard was written.
    /// </summary>
    internal static Expression? TryShortCircuit(BinaryExpression binary)
    {
        if (binary.NodeType is not (ExpressionType.AndAlso or ExpressionType.OrElse)) return null;
        if (!TryEvaluate(binary.Left, out var left)) return null;

        return left == (binary.NodeType == ExpressionType.AndAlso) ? binary.Right : Expression.Constant(left);
    }

    private sealed class ParameterFinder : ExpressionVisitor
    {
        private bool _found;

        public static bool ReferencesParameter(Expression expression)
        {
            var finder = new ParameterFinder();
            finder.Visit(expression);
            return finder._found;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            _found = true;
            return node;
        }
    }

    private static bool IsDocumentMember(MemberExpression expression)
    {
        // Walk up the chain to find if it originates from a ParameterExpression
        var current = expression;
        while (current != null)
        {
            if (current.Expression is ParameterExpression)
                return true;

            current = current.Expression as MemberExpression;
        }

        return false;
    }

    private static bool IsBooleanMember(MemberExpression expression)
    {
        var memberType = expression.Member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => null
        };

        return memberType == typeof(bool) && IsDocumentMember(expression);
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static string ReverseOperator(string op)
    {
        return op switch
        {
            ">" => "<",
            ">=" => "<=",
            "<" => ">",
            "<=" => ">=",
            _ => op // = and != are symmetric
        };
    }
}
