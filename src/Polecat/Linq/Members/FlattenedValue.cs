using System.Linq.Expressions;

namespace Polecat.Linq.Members;

/// <summary>
///     gh-1016: stands in for one element of a flattened collection of scalars, so that every rule written for
///     a member applies to it. <c>SelectMany(x =&gt; x.Words).Where(w =&gt; w.Length == 6)</c> is read as
///     <c>e.Value.Length == 6</c> over one of these, and <c>e.Value</c> resolves to the element's value.
/// </summary>
/// <remarks>
///     Never instantiated: it exists to be named in an expression tree, which is rewritten back to the element
///     itself before anything is compiled.
/// </remarks>
internal sealed class FlattenedValue<T>
{
    public T Value { get; set; } = default!;
}

internal static class FlattenedValue
{
    /// <summary>Whether <paramref name="expression" /> is <c>e.Value</c> over a <see cref="FlattenedValue{T}" />.</summary>
    public static bool IsValueOf(MemberExpression expression)
        => expression is { Member.Name: "Value", Expression: ParameterExpression parameter }
           && IsStandIn(parameter.Type);

    public static bool IsStandIn(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(FlattenedValue<>);
}
