using JasperFx.Core.Reflection;

namespace Polecat.Linq.Members;

/// <summary>
///     Represents the document's Id property, mapped directly to the "id" column.
/// </summary>
internal class IdMember : IQueryableMember
{
    private readonly ValueTypeInfo? _valueTypeInfo;

    public IdMember(Type idType, ValueTypeInfo? valueTypeInfo = null)
    {
        MemberType = idType;
        _valueTypeInfo = valueTypeInfo;
    }

    public Type MemberType { get; }
    public string TypedLocator => "id";
    public string RawLocator => "id";
    public bool IsBoolean => false;

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
