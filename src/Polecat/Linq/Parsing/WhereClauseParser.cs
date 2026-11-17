using JasperFx;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing.Methods;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Parses a predicate expression tree into an ISqlFragment for the WHERE clause.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: evaluates constant sub-expressions in predicates via Expression.Lambda + Compile — runtime code generation. Document/event types and their property graph are preserved at the LINQ registration boundary on the caller side per the AOT publishing guide.")]
internal class WhereClauseParser
{
    private static readonly FrozenDictionary<ExpressionType, string> Operators = new Dictionary<ExpressionType, string>()
    {
        { ExpressionType.Equal, "=" },
        { ExpressionType.NotEqual, "!=" },
        { ExpressionType.GreaterThan, ">" },
        { ExpressionType.GreaterThanOrEqual, ">=" },
        { ExpressionType.LessThan, "<" },
        { ExpressionType.LessThanOrEqual, "<=" }
    }.ToFrozenDictionary();

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
            // gh-1014: x.Flags[1], a bool read through an index (an ArrayIndex is a BinaryExpression too)
            _ when expression.Type == typeof(bool) && DocumentPaths.IsIndexPath(expression)
                => ParseBooleanPath(expression, true),
            BinaryExpression binary => ParseBinary(binary),
            UnaryExpression { NodeType: ExpressionType.Not } unary => ParseNot(unary),
            UnaryExpression { NodeType: ExpressionType.Convert } unary => ParsePredicate(unary.Operand),
            MethodCallExpression methodCall => ParseMethodCall(methodCall),
            MemberExpression member when IsBooleanMember(member) => ParseBooleanMember(member, true),
            TypeBinaryExpression { NodeType: ExpressionType.TypeIs } typeIs => ParseTypeIs(typeIs),
            _ => throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to a SQL WHERE clause ({expression.NodeType}, "
                + $"{expression.GetType().Name}). A predicate is built from member comparisons "
                + "(x.Name == \"Bob\"), && and || over those, !, a boolean member, and the supported method "
                + $"calls ({MethodCallParserRegistry.SupportedCalls}). To go outside that, run the "
                + "query as raw SQL through session.AdvancedSql.QueryAsync<T>(...), or materialize it and "
                + "filter the results in memory.")
        };
    }

    /// <summary>
    ///     #808: <c>x.Payment.Value is Card</c>, the one type test SQL can answer: which case a C#
    ///     union-typed member holds. The compiler refuses the union pattern <c>x.Payment is Card</c> in an
    ///     expression tree (CS8122), which is why the test goes through <c>Value</c>.
    /// </summary>
    private ISqlFragment ParseTypeIs(TypeBinaryExpression typeIs)
    {
        if (StripConvert(typeIs.Expression) is MemberExpression member && UnionMembers.IsUnionValue(member)
            && IsDocumentMember(member))
        {
            return _memberFactory.ResolveUnionTypeTest(member, typeIs.TypeOperand, typeIs);
        }

        throw new BadLinqExpressionException(
            $"Polecat cannot translate the type test '{typeIs}' to SQL. The stored JSON carries no .NET "
            + "type, so the only type test it can answer is which case a C# union-typed member holds "
            + "(x.Payment.Value is Card). Materialize the query and test the type in memory.");
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
        // #808: (x.Payment.Value as Card) == null is a type test: the cast is null exactly when the union
        // holds another case (or none).
        if (TryParseCaseNullTest(binary.Left, binary.Right, op, out var caseTest)
            || TryParseCaseNullTest(binary.Right, binary.Left, op, out caseTest))
        {
            return caseTest!;
        }

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

        if (TryParseComputedComparison(binary, op, out var computedFragment, out var reason))
        {
            return computedFragment!;
        }

        throw new BadLinqExpressionException(
            $"Polecat cannot translate the comparison '{binary}' to SQL"
            + (string.IsNullOrEmpty(reason) ? "" : $": {reason}")
            + ". Compare a member, or a value computed from members with arithmetic, concatenation, ToUpper(), "
            + "ToLower() or Length, to a value (x.Age > 30), or run the query as raw SQL through "
            + "session.AdvancedSql.QueryAsync<T>(...).");
    }

    private bool TryParseCaseNullTest(Expression castSide, Expression valueSide, string op,
        out ISqlFragment? fragment)
    {
        fragment = null;
        if (op is not ("=" or "!=")) return false;
        if (!UnionMembers.TryGetCaseCast(castSide, out var unionValue, out var caseType)) return false;
        if (!IsDocumentMember(unionValue) || ParameterFinder.ReferencesParameter(valueSide)) return false;

        // A primitive case compared to a value is an ordinary comparison, below.
        if (ExtractValue(valueSide) != null) return false;

        var test = _memberFactory.ResolveUnionTypeTest(unionValue, caseType, castSide);
        fragment = op == "!=" ? test : new NotFragment(test);
        return true;
    }

    private bool TryParseMethodTransform(Expression methodSide, Expression valueSide, string op,
        out ISqlFragment? fragment)
    {
        fragment = null;

        var unwrapped = StripConvert(methodSide);
        if (unwrapped is not MethodCallExpression methodCall) return false;

        // x.Name == input.ToLower() is the value side: there is no document in it to transform. And a value
        // side that reads the document is two values of the row compared, for TryParseComputedComparison.
        if (!ParameterFinder.ReferencesParameter(methodCall) || ParameterFinder.ReferencesParameter(valueSide))
        {
            return false;
        }

        // #1030: x.Email.ToLower() == value over a Casing = Lower index is a comparison with that index's
        // column, which seeks, rather than with LOWER() over the document, which scans.
        if (TryResolveCasedMember(methodCall) is { } cased)
        {
            fragment = BuildMemberComparison(cased, ExtractValue(valueSide), op);
            return true;
        }

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
        else if (op is "=" or "!=" && value is string)
        {
            fragment = new TextEqualityFilter(funcLocator.FullLocator,
                TextEqualityFilter.Measured(funcLocator.FullLocator), op == "=", value);
        }
        else
        {
            fragment = new ComparisonFilter(funcLocator.FullLocator, op, value);
        }

        return true;
    }

    /// <summary>
    ///     #1030: the member a <c>ToLower()</c> or <c>ToUpper()</c> folds, read through an index of the same
    ///     casing, or null when the call is anything else or no such index covers the member.
    /// </summary>
    private IQueryableMember? TryResolveCasedMember(MethodCallExpression call)
    {
        if (call.Method.DeclaringType != typeof(string) || call.Arguments.Count != 0) return null;

        var casing = call.Method.Name switch
        {
            nameof(string.ToLower) or nameof(string.ToLowerInvariant) => IndexCasing.Lower,
            nameof(string.ToUpper) or nameof(string.ToUpperInvariant) => IndexCasing.Upper,
            _ => IndexCasing.Default
        };

        if (casing == IndexCasing.Default) return null;
        if (call.Object == null || StripConvert(call.Object) is not MemberExpression member
                                || !IsDocumentMember(member))
        {
            return null;
        }

        return _memberFactory.ResolveCasedMember(member, casing);
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
        if (ParameterFinder.ReferencesParameter(valueSide)) return false;

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

        // Two values of the row compared, or a computed divisor: TryParseComputedComparison
        if (ParameterFinder.ReferencesParameter(binary.Left) && ParameterFinder.ReferencesParameter(binary.Right))
        {
            return false;
        }

        // Pattern: (x.Number % divisor) op value
        if (binary.Left is BinaryExpression { NodeType: ExpressionType.Modulo } modulo
            && !ParameterFinder.ReferencesParameter(modulo.Right))
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
        if (binary.Right is BinaryExpression { NodeType: ExpressionType.Modulo } moduloR
            && !ParameterFinder.ReferencesParameter(moduloR.Right))
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
        // #808: a case member is compared only on rows holding that case. Its locator is NULL on every
        // other row, which a null-inclusive != or an == null would otherwise read as a match.
        if (member is UnionCaseMember caseMember)
        {
            return new CompoundWhereFragment("AND", new WhereFragment(caseMember.Guard),
                BuildMemberComparison(member, value, op));
        }

        return BuildMemberComparison(member, value, op);
    }

    private ISqlFragment BuildMemberComparison(IQueryableMember member, object? value, string op)
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
                ? new WhereFragment($"{member.NullLocator} IS NULL")
                : new WhereFragment($"{member.NullLocator} IS NOT NULL");
        }

        return member.Comparison(op, IsPositive, value);
    }

    private ISqlFragment ParseNot(UnaryExpression unary)
    {
        var operand = unary.Operand;

        // !x.IsActive
        if (operand is MemberExpression member && IsBooleanMember(member))
        {
            return ParseBooleanMember(member, false);
        }

        if (operand.Type == typeof(bool) && DocumentPaths.IsIndexPath(operand))
        {
            return ParseBooleanPath(operand, false);
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

    private ISqlFragment ParseBooleanPath(Expression path, bool expectedValue)
    {
        var resolved = _memberFactory.ResolvePath(path);
        return new ComparisonFilter(resolved.TypedLocator, "=", resolved.ConvertValue(expectedValue)!);
    }

    private bool TryResolveMemberAndValue(Expression memberExpr, Expression valueExpr,
        out IQueryableMember? member, out object? value)
    {
        member = null;
        value = null;

        // #808: (int)x.Code.Value > 3 reads the union's primitive case as an int, which StripConvert
        // would throw away along with the type to compare it as.
        if (UnionMembers.TryGetCaseCast(memberExpr, out var unionValue, out var caseType)
            && IsDocumentMember(unionValue))
        {
            member = _memberFactory.ResolveUnionCase(unionValue, caseType, memberExpr);
            value = ExtractValue(valueExpr);
            return true;
        }

        var unwrappedMember = StripConvert(memberExpr);

        if (unwrappedMember is MemberExpression me && IsDocumentMember(me))
        {
            if (ParameterFinder.ReferencesParameter(valueExpr)) return false;

            member = _memberFactory.ResolveMember(me);
            value = ExtractValue(valueExpr);
            return true;
        }

        // gh-1014: x.Values[1] == 3, x.Grid[1][0] > 6
        if (DocumentPaths.IsIndexPath(unwrappedMember) && !ParameterFinder.ReferencesParameter(valueExpr))
        {
            member = _memberFactory.ResolvePath(unwrappedMember);
            value = ExtractValue(valueExpr);
            return true;
        }

        return false;
    }

    /// <summary>
    ///     gh-1012 / gh-1017 (marten#5942): a comparison the patterns above do not cover, such as a computed
    ///     value against a constant (<c>x.Number * 2 &gt; 4</c>, which is what <c>Select(x =&gt; x.Number * 2)
    ///     .Where(n =&gt; n &gt; 4)</c> reads as) or two values of the row against each other
    ///     (<c>x.Grid[0][0] &lt; x.Values[1]</c>). Each side reading the row is translated by
    ///     <see cref="ScalarTranslation" /> or the comparison is refused; the other side is bound.
    /// </summary>
    private bool TryParseComputedComparison(BinaryExpression binary, string op, out ISqlFragment? fragment,
        out string? reason)
    {
        fragment = null;
        reason = null;

        var leftReads = ParameterFinder.ReferencesParameter(binary.Left);
        var rightReads = ParameterFinder.ReferencesParameter(binary.Right);
        if (!leftReads && !rightReads) return false;

        if (leftReads && rightReads)
        {
            if (!ScalarTranslation.TryTranslate(binary.Left, _memberFactory, out var left, out reason)
                || !ScalarTranslation.TryTranslate(binary.Right, _memberFactory, out var right, out reason))
            {
                return false;
            }

            // = pads the shorter text with spaces; the byte counts keep 'a' and 'a ' apart, as .NET does
            var leftBytes = $"DATALENGTH({TextEqualityFilter.Measured(left.Sql)})";
            var rightBytes = $"DATALENGTH({TextEqualityFilter.Measured(right.Sql)})";
            fragment = op is "=" or "!=" && left.Type == typeof(string)
                ? new WhereFragment(op == "="
                    ? $"({left.Sql} = {right.Sql} AND {leftBytes} = {rightBytes})"
                    : $"({left.Sql} != {right.Sql} OR {leftBytes} != {rightBytes})")
                : new WhereFragment($"{left.Sql} {op} {right.Sql}");
            return true;
        }

        var computed = leftReads ? binary.Left : binary.Right;
        var valueSide = leftReads ? binary.Right : binary.Left;
        if (!leftReads) op = ReverseOperator(op);

        if (!ScalarTranslation.TryTranslate(computed, _memberFactory, out var sql, out reason)) return false;

        var value = ExtractValue(valueSide);
        if (value == null)
        {
            if (op is not ("=" or "!=")) return false;
            fragment = new WhereFragment(op == "=" ? $"{sql.Sql} IS NULL" : $"{sql.Sql} IS NOT NULL");
            return true;
        }

        // = and != over text pad trailing spaces; measuring the text keeps 'a' and 'a ' apart, as .NET does
        fragment = op is "=" or "!=" && value is string
            ? new TextEqualityFilter(sql.Sql, TextEqualityFilter.Measured(sql.Sql), op == "=", value)
            : new ComparisonFilter(sql.Sql, op, value);
        return true;
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
        // The compiler's own answer is that a variable is not defined in its scope.
        if (ParameterFinder.ReferencesParameter(expression))
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to SQL as a value: it reads the document, and a "
                + "comparison is between a document member and something known before the query runs. Two "
                + "members cannot be compared, so run the query as raw SQL through "
                + "session.AdvancedSql.QueryAsync<T>(...), or materialize it and filter in memory.");
        }

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
        private readonly HashSet<ParameterExpression> _bound = [];
        private bool _found;

        public static bool ReferencesParameter(Expression expression)
        {
            var finder = new ParameterFinder();
            finder.Visit(expression);
            return finder._found;
        }

        // A nested lambda binds its own parameters, as in captured.Where(e => e.IsArchived), and that runs
        // in memory. Only a parameter nothing here binds is the row the query is reading.
        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            _bound.UnionWith(node.Parameters);
            return base.VisitLambda(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            _found |= !_bound.Contains(node);
            return node;
        }
    }

    // #808: ((Card)x.Payment.Value).Last4 is read off the document through a union's case, and gh-1014:
    // x.Items[0].Name through an index.
    private static bool IsDocumentMember(MemberExpression expression) => DocumentPaths.IsRooted(expression);

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
