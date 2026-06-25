using System.Text.Json;
using Weasel.Core;

namespace Polecat.Linq.Members;

/// <summary>
///     A queryable member for enum properties, aware of the enum storage mode.
/// </summary>
internal class EnumMember : IQueryableMember
{
    private readonly EnumStorage _enumStorage;
    private readonly JsonNamingPolicy? _namingPolicy;

    public EnumMember(string rawLocator, Type memberType, EnumStorage enumStorage,
        JsonNamingPolicy? namingPolicy = null)
    {
        RawLocator = rawLocator;
        MemberType = memberType;
        _enumStorage = enumStorage;
        _namingPolicy = namingPolicy;

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

        if (_enumStorage != EnumStorage.AsString)
        {
            return Convert.ToInt32(value);
        }

        // #222: AsString enum names are serialized through the configured JsonNamingPolicy
        // (the default Serializer wires JsonStringEnumConverter(PropertyNamingPolicy)), so a
        // Minute member is stored as "minute" under Casing.CamelCase. The predicate literal
        // must apply the SAME policy or it compares 'Minute' against 'minute' and matches
        // nothing. ToString() (raw member name) is correct only for the identity policy.
        //
        // The value can arrive three ways depending on how the LINQ provider lowered the node:
        // an already-resolved string, the enum instance itself (e.g. list.Contains paths), or —
        // for an equality predicate — the enum constant *converted to its underlying integer*.
        // Resolve all three back to the member name before applying the policy.
        string? name;
        if (value is string s)
        {
            name = s;
        }
        else if (value.GetType().IsEnum)
        {
            name = value.ToString();
        }
        else
        {
            // Underlying integer form: map back to the member name. Undefined values
            // (Enum.GetName == null) fall back to the raw literal rather than throwing.
            name = Enum.GetName(MemberType, value) ?? value.ToString();
        }

        return _namingPolicy != null && name != null
            ? _namingPolicy.ConvertName(name)
            : name;
    }
}
