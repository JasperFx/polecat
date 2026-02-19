using Polecat.Serialization;

namespace Polecat.Linq.Members;

/// <summary>
///     A queryable member for enum properties, aware of the enum storage mode.
/// </summary>
internal class EnumMember : IQueryableMember
{
    private readonly EnumStorage _enumStorage;

    public EnumMember(string rawLocator, Type memberType, EnumStorage enumStorage)
    {
        RawLocator = rawLocator;
        MemberType = memberType;
        _enumStorage = enumStorage;

        TypedLocator = enumStorage == EnumStorage.AsInteger
            ? $"CAST({rawLocator} AS int)"
            : rawLocator;
    }

    public Type MemberType { get; }
    public string TypedLocator { get; }
    public string RawLocator { get; }
    public bool IsBoolean => false;

    public object? ConvertValue(object? value)
    {
        if (value == null) return null;

        return _enumStorage == EnumStorage.AsString
            ? value.ToString()
            : Convert.ToInt32(value);
    }
}
