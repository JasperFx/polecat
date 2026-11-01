namespace Polecat.Linq.Members;

/// <summary>
///     A value SQL computes from a stored member rather than reads from the document, such as
///     <c>x.CreatedAt.Year</c> or <c>x.Items.Count</c>. See <see cref="ClrOnlyMembers" />.
/// </summary>
internal sealed class ComputedMember : IQueryableMember
{
    public ComputedMember(string locator, Type memberType, string sqlType, bool isSubquery = false)
    {
        TypedLocator = locator;
        MemberType = memberType;
        LocatorSqlType = sqlType;
        IsSubquery = isSubquery;
    }

    public Type MemberType { get; }
    public string TypedLocator { get; }
    public string RawLocator => TypedLocator;
    public bool IsBoolean => false;
    public string? LocatorSqlType { get; }

    /// <summary>
    ///     Whether the locator is a subquery, which SQL Server refuses in a GROUP BY key and inside an
    ///     aggregate.
    /// </summary>
    public bool IsSubquery { get; }

    /// <summary>
    ///     <c>DayOfWeek</c> is the one computed value with an enum on the CLR side, compared as its number.
    /// </summary>
    public object? ConvertValue(object? value) => value is Enum ? Convert.ToInt32(value) : value;
}
