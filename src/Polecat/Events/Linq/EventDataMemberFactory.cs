using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Patching;
using Polecat.Serialization;
using Polecat.Storage;
using Weasel.Core;

namespace Polecat.Events.Linq;

/// <summary>
///     Resolves event data type properties to JSON_VALUE expressions on the data column.
///     Unlike MemberFactory, all properties (including Id) resolve to JSON paths.
/// </summary>
internal class EventDataMemberFactory : IMemberResolver
{
    private readonly JsonNamingPolicy? _namingPolicy;
    private readonly EnumStorage _enumStorage;
    private readonly bool _useReturning;
    private readonly ISerializer _serializer;

    public EventDataMemberFactory(StoreOptions options)
    {
        _enumStorage = options.Serializer.EnumStorage;
        _serializer = options.Serializer;
        _useReturning = options.UseNativeJsonType;

        if (options.Serializer is Serializer s)
        {
            _namingPolicy = s.Options.PropertyNamingPolicy;
        }
        else
        {
            _namingPolicy = JsonNamingPolicy.CamelCase;
        }
    }

    public IQueryableMember ResolveMember(MemberExpression expression)
    {
        // #840: the same rule as MemberFactory, because an event body is JSON too.
        return ClrOnlyMembers.TryTranslate(expression, ResolveMember)
               ?? CreateMember(BuildJsonPath(expression), GetMemberType(expression.Member));
    }

    public IQueryableMember ResolveKey(MemberExpression expression, string position)
        => StoredShape.AssertHasValue(ResolveMember(expression), expression.Member, _serializer, position);

    private IQueryableMember CreateMember(string jsonPath, Type memberType)
    {
        var rawLocator = $"JSON_VALUE(data, '{jsonPath}')";
        var underlying = Nullable.GetUnderlyingType(memberType) ?? memberType;

        if (underlying.IsEnum)
        {
            return new EnumMember(rawLocator, underlying, _enumStorage, _namingPolicy);
        }

        if (underlying == typeof(bool))
        {
            return new QueryableMember(rawLocator, rawLocator, memberType, isBoolean: true);
        }

        var sqlType = GetSqlType(underlying);
        var typedLocator = sqlType != null
            ? $"CAST({rawLocator} AS {sqlType})"
            : rawLocator;

        // #835: the same read the document side uses, because the text predicates are shared.
        return underlying == typeof(string)
            ? new QueryableMember(SqlText.UnlimitedJsonString("data", jsonPath, _useReturning), typedLocator,
                memberType, textCapacity: SqlText.JsonValueCapacity, bareTextLocator: rawLocator)
            : new QueryableMember(rawLocator, typedLocator, memberType,
                mayBeObject: !SelectProjectionAnalyzer.IsScalar(underlying));
    }

    // #916: through the rule the document side uses, which is also what reads [JsonPropertyName]. An event
    // body is written by the same serializer, so a renamed member was simply absent here.
    private string BuildJsonPath(MemberExpression expression)
        => SerializedNames.PathFor(
            ClrOnlyMembers.JsonKeys(expression).Select(x => JsonPathHelper.FormatMember(x, _namingPolicy)), _serializer);

    private static Type GetMemberType(MemberInfo member)
    {
        return member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new JasperFx.BadLinqExpressionException(
                $"Polecat cannot translate the member '{member.Name}': a translatable member is a "
                + $"property or a field, not a {member.MemberType}.")
        };
    }

    private static string? GetSqlType(Type type) => SqlTypeMap.For(type);
}
