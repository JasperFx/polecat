using JasperFx.Core.Reflection;

namespace Polecat.Linq.Members;

/// <summary>
///     Represents the document's Id property, mapped directly to the "id" column.
/// </summary>
internal class IdMember : IQueryableMember
{
    private readonly ValueTypeInfo? _valueTypeInfo;

    public IdMember(Type idType, ValueTypeInfo? valueTypeInfo = null, string? locatorSqlType = null)
    {
        MemberType = idType;
        _valueTypeInfo = valueTypeInfo;
        LocatorSqlType = locatorSqlType;
    }

    public Type MemberType { get; }
    public string TypedLocator => "id";
    public string RawLocator => "id";
    public bool IsBoolean => false;

    /// <summary>
    ///     The id COLUMN's own type, not the member's. A strong-typed id's wrapper has no SQL type;
    ///     the column holds its inner scalar (#296/#302).
    /// </summary>
    public string? LocatorSqlType { get; }

    public object? ConvertValue(object? value)
    {
        if (value == null) return null;
        if (_valueTypeInfo != null)
        {
            return _valueTypeInfo.ValueProperty.GetValue(value);
        }

        return value;
    }
}
