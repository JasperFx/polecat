namespace Polecat.Linq.Members;

/// <summary>
///     A queryable member backed by JSON_VALUE(data, '$.path').
/// </summary>
internal class QueryableMember : IQueryableMember
{
    public QueryableMember(string rawLocator, string typedLocator, Type memberType, bool isBoolean = false,
        string? locatorSqlType = null, int? textCapacity = null)
    {
        RawLocator = rawLocator;
        TypedLocator = typedLocator;
        MemberType = memberType;
        IsBoolean = isBoolean;
        LocatorSqlType = locatorSqlType;
        TextCapacity = textCapacity;
    }

    public Type MemberType { get; }
    public string TypedLocator { get; }
    public string RawLocator { get; }
    public bool IsBoolean { get; }
    public string? LocatorSqlType { get; }
    public int? TextCapacity { get; }

    public object? ConvertValue(object? value)
    {
        if (value == null) return null;
        if (IsBoolean) return (bool)value ? "true" : "false";
        return value;
    }
}
