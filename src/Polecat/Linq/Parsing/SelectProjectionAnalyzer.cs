using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;

namespace Polecat.Linq.Parsing;

/// <summary>
///     One projected column of a "simple" <c>Select()</c> projection: the emitted JSON key and the
///     SQL locator that produces its value.
/// </summary>
internal sealed record SimpleProjectionColumn(string JsonKey, string Locator, Type MemberType);

/// <summary>
///     Detects whether a <c>Select()</c> projection is "simple" — composed only of (optionally
///     nested) scalar member accesses, with no method calls, arithmetic, or conditionals — and, if
///     so, returns the key/locator columns needed to build a server-side JSON object. Mirrors
///     Marten's <c>SelectParser</c> "simple projection" detection (JasperFx/marten#5017), adopting
///     the #5031 refinement that strips safe/widening conversions (int→long, T→object, enum→int,
///     non-null→nullable) rather than over-bailing on them.
/// </summary>
internal static class SelectProjectionAnalyzer
{
    /// <summary>
    ///     Returns the projected columns if <paramref name="select"/> is a simple object projection
    ///     (anonymous type or DTO with member assignments, every value a scalar member access), or
    ///     <c>null</c> when it is not translatable to a server-side JSON object.
    /// </summary>
    public static IReadOnlyList<SimpleProjectionColumn>? TryAnalyze(
        LambdaExpression select, MemberFactory memberFactory)
    {
        var body = StripSafeConvert(select.Body);

        return body switch
        {
            NewExpression newExpression => AnalyzeNew(newExpression, memberFactory),
            MemberInitExpression memberInit => AnalyzeMemberInit(memberInit, memberFactory),
            _ => null
        };
    }

    private static IReadOnlyList<SimpleProjectionColumn>? AnalyzeNew(
        NewExpression newExpression, MemberFactory memberFactory)
    {
        // Anonymous types (and record-style positional ctors) expose the projected member names via
        // NewExpression.Members. Without them we cannot name the JSON keys → not translatable.
        if (newExpression.Members is null || newExpression.Members.Count != newExpression.Arguments.Count)
        {
            return null;
        }

        var columns = new List<SimpleProjectionColumn>(newExpression.Arguments.Count);
        for (var i = 0; i < newExpression.Arguments.Count; i++)
        {
            var column = TryColumn(newExpression.Members[i], newExpression.Arguments[i], memberFactory);
            if (column is null) return null;
            columns.Add(column);
        }

        return columns;
    }

    private static IReadOnlyList<SimpleProjectionColumn>? AnalyzeMemberInit(
        MemberInitExpression memberInit, MemberFactory memberFactory)
    {
        // Only a parameterless `new Dto { A = x.A, ... }` shape is simple.
        if (memberInit.NewExpression.Arguments.Count != 0) return null;

        var columns = new List<SimpleProjectionColumn>(memberInit.Bindings.Count);
        foreach (var binding in memberInit.Bindings)
        {
            if (binding is not MemberAssignment assignment) return null;

            var column = TryColumn(assignment.Member, assignment.Expression, memberFactory);
            if (column is null) return null;
            columns.Add(column);
        }

        return columns.Count > 0 ? columns : null;
    }

    private static SimpleProjectionColumn? TryColumn(
        MemberInfo projectionMember, Expression valueExpression, MemberFactory memberFactory)
    {
        var value = StripSafeConvert(valueExpression);

        // The value must be a (possibly nested) member access rooted at the lambda parameter.
        if (value is not MemberExpression memberExpression || !IsRootedAtParameter(memberExpression))
        {
            return null;
        }

        var member = memberFactory.ResolveMember(memberExpression);

        // Only scalar leaf members translate cleanly to a JSON_VALUE locator. Projecting a whole
        // nested object/array would need JSON_QUERY handling → treat as not simple.
        if (!IsScalar(member.MemberType)) return null;

        var key = memberFactory.FormatProjectedKey(projectionMember);
        return new SimpleProjectionColumn(key, member.TypedLocator, member.MemberType);
    }

    private static bool IsRootedAtParameter(MemberExpression expression)
    {
        Expression? current = expression;
        while (current is MemberExpression m)
        {
            current = m.Expression;
        }

        return current is ParameterExpression;
    }

    private static bool IsScalar(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsPrimitive
               || underlying.IsEnum
               || underlying == typeof(string)
               || underlying == typeof(Guid)
               || underlying == typeof(decimal)
               || underlying == typeof(DateTime)
               || underlying == typeof(DateTimeOffset)
               || underlying == typeof(TimeSpan)
               || underlying == typeof(DateOnly)
               || underlying == typeof(TimeOnly);
    }

    // Strip no-op / safe / widening conversions (marten#5031 fixed behavior): boxing to object,
    // widening numeric conversions, enum <-> underlying, and non-null -> nullable. Bail (leave the
    // node in place, so the caller treats the projection as non-simple) on lossy/narrowing casts.
    private static Expression StripSafeConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
        {
            if (!IsSafeConversion(convert.Operand.Type, convert.Type)) break;
            expression = convert.Operand;
        }

        return expression;
    }

    private static bool IsSafeConversion(Type from, Type to)
    {
        if (to == from) return true;
        if (to == typeof(object)) return true;

        // non-null T -> Nullable<T>
        if (Nullable.GetUnderlyingType(to) == from) return true;

        var fromUnderlying = Nullable.GetUnderlyingType(from) ?? from;
        var toUnderlying = Nullable.GetUnderlyingType(to) ?? to;

        // enum <-> its underlying integral type
        if (fromUnderlying.IsEnum && Enum.GetUnderlyingType(fromUnderlying) == toUnderlying) return true;
        if (toUnderlying.IsEnum && Enum.GetUnderlyingType(toUnderlying) == fromUnderlying) return true;

        // widening numeric conversions
        return IsWideningNumeric(fromUnderlying, toUnderlying);
    }

    private static bool IsWideningNumeric(Type from, Type to)
    {
        if (!WideningRank.TryGetValue(from, out var fromRank)) return false;
        if (!WideningRank.TryGetValue(to, out var toRank)) return false;
        return toRank >= fromRank;
    }

    // Coarse widening ranks: a conversion is widening when the target rank >= the source rank within
    // the same signed/float family. Kept intentionally conservative — anything not covered is treated
    // as non-simple rather than risking a lossy cast being silently translated.
    private static readonly Dictionary<Type, int> WideningRank = new()
    {
        [typeof(sbyte)] = 1,
        [typeof(short)] = 2,
        [typeof(int)] = 3,
        [typeof(long)] = 4,
        [typeof(byte)] = 1,
        [typeof(ushort)] = 2,
        [typeof(uint)] = 3,
        [typeof(ulong)] = 4,
        [typeof(float)] = 5,
        [typeof(double)] = 6,
    };
}
