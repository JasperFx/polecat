using Polecat.Internal;
using Polecat.Linq.Parsing;

namespace Polecat.Linq.Members;

/// <summary>
///     A queryable member backed by JSON_VALUE(data, '$.path').
/// </summary>
internal class QueryableMember : IQueryableMember
{
    public QueryableMember(string rawLocator, string typedLocator, Type memberType, bool isBoolean = false,
        string? locatorSqlType = null, int? textCapacity = null, string? bareTextLocator = null,
        bool mayBeObject = false)
    {
        RawLocator = rawLocator;
        TypedLocator = typedLocator;
        MemberType = memberType;
        IsBoolean = isBoolean;
        LocatorSqlType = locatorSqlType;
        TextCapacity = textCapacity;
        BareTextLocator = bareTextLocator;
        MayBeObject = mayBeObject;
    }

    public Type MemberType { get; }
    public string TypedLocator { get; }
    public string RawLocator { get; }
    public bool IsBoolean { get; }
    public string? LocatorSqlType { get; }
    public int? TextCapacity { get; }
    public string? BareTextLocator { get; }
    public bool MayBeObject { get; }

    public string NullLocator => MayBeObject ? SqlText.StoredValue(RawLocator) : RawLocator;

    public object? ConvertValue(object? value)
    {
        if (value == null) return null;

        // #875: SqlClient would refuse it with no mapping for the type, which names neither the member
        // nor the query. A member typed object can still hold a scalar, and be compared to one.
        if (MayBeObject && !SelectProjectionAnalyzer.IsScalar(value.GetType()))
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot compare '{RawLocator}' to a {value.GetType().Name}: a JSON object or array "
                + "has no value SQL can compare. Compare its members, check it against null, or run the "
                + "query as raw SQL through session.AdvancedSql.QueryAsync<T>(...).");
        }

        if (IsBoolean) return (bool)value ? "true" : "false";
        return value;
    }
}
