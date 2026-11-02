using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Members;

/// <summary>
///     <c>x.N.HasValue</c>. A predicate reads it as <c>IS NOT NULL</c> through <see cref="Is" />, and
///     anywhere a value is needed (a Select, an OrderBy, a GroupBy key) it yields the text
///     <c>'true'</c> or <c>'false'</c>, exactly as a stored bool's locator does.
/// </summary>
internal sealed class HasValueMember : IQueryableMember
{
    private readonly string _stored;

    /// <param name="nullable">The <see cref="Nullable{T}" /> member this is read off.</param>
    public HasValueMember(IQueryableMember nullable) => _stored = nullable.NullLocator;

    public Type MemberType => typeof(bool);
    public string TypedLocator => $"CASE WHEN {_stored} IS NOT NULL THEN 'true' ELSE 'false' END";
    public string RawLocator => TypedLocator;
    public bool IsBoolean => true;
    public string? LocatorSqlType => null;

    public object? ConvertValue(object? value)
        => value is bool hasValue ? hasValue ? "true" : "false" : value;

    /// <summary>The predicate that <c>HasValue</c> is <paramref name="expected" />.</summary>
    public ISqlFragment Is(bool expected)
        => new WhereFragment(expected ? $"{_stored} IS NOT NULL" : $"{_stored} IS NULL");
}
