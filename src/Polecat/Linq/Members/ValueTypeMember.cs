using JasperFx.Core.Reflection;

namespace Polecat.Linq.Members;

/// <summary>
///     A document member whose declared type is a strong-typed-id style wrapper (Vogen,
///     StronglyTypedId, or a hand-written <c>record struct</c>) sitting somewhere other than the Id.
///     The locator is typed from the wrapper's inner scalar — both generators serialize the inner
///     value, so the JSON holds a scalar — and <see cref="ConvertValue" /> unwraps the CLR value so
///     SqlClient binds the inner scalar rather than a type it has no mapping for.
/// </summary>
internal class ValueTypeMember : IQueryableMember
{
    private readonly ValueTypeInfo _valueType;

    public ValueTypeMember(string rawLocator, string typedLocator, Type memberType, ValueTypeInfo valueType)
    {
        RawLocator = rawLocator;
        TypedLocator = typedLocator;
        MemberType = memberType;
        _valueType = valueType;
    }

    public Type MemberType { get; }
    public string TypedLocator { get; }
    public string RawLocator { get; }
    public bool IsBoolean => false;

    public object? ConvertValue(object? value)
        => value == null ? null : _valueType.ValueProperty.GetValue(value);
}
