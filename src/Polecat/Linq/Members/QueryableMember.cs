namespace Polecat.Linq.Members;

/// <summary>
///     A queryable member backed by JSON_VALUE(data, '$.path').
/// </summary>
internal class QueryableMember : IQueryableMember
{
    public QueryableMember(string rawLocator, string typedLocator, Type memberType, bool isBoolean = false)
    {
        RawLocator = rawLocator;
        TypedLocator = typedLocator;
        MemberType = memberType;
        IsBoolean = isBoolean;
    }

    public Type MemberType { get; }
    public string TypedLocator { get; }
    public string RawLocator { get; }
    public bool IsBoolean { get; }

    public object? ConvertValue(object? value)
    {
        if (value == null) return null;
        if (IsBoolean) return (bool)value ? "true" : "false";
        return value;
    }
}
