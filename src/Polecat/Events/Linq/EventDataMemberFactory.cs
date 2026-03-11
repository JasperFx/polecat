using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Polecat.Linq.Members;
using Polecat.Serialization;

namespace Polecat.Events.Linq;

/// <summary>
///     Resolves event data type properties to JSON_VALUE expressions on the data column.
///     Unlike MemberFactory, all properties (including Id) resolve to JSON paths.
/// </summary>
internal class EventDataMemberFactory : IMemberResolver
{
    private readonly JsonNamingPolicy? _namingPolicy;
    private readonly EnumStorage _enumStorage;

    public EventDataMemberFactory(StoreOptions options)
    {
        _enumStorage = options.Serializer.EnumStorage;

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
        var jsonPath = BuildJsonPath(expression);
        var memberType = GetMemberType(expression.Member);
        return CreateMember(jsonPath, memberType);
    }

    private IQueryableMember CreateMember(string jsonPath, Type memberType)
    {
        var rawLocator = $"JSON_VALUE(data, '{jsonPath}')";
        var underlying = Nullable.GetUnderlyingType(memberType) ?? memberType;

        if (underlying.IsEnum)
        {
            return new EnumMember(rawLocator, underlying, _enumStorage);
        }

        if (underlying == typeof(bool))
        {
            return new QueryableMember(rawLocator, rawLocator, memberType, isBoolean: true);
        }

        var sqlType = GetSqlType(underlying);
        var typedLocator = sqlType != null
            ? $"CAST({rawLocator} AS {sqlType})"
            : rawLocator;

        return new QueryableMember(rawLocator, typedLocator, memberType);
    }

    private string BuildJsonPath(MemberExpression expression)
    {
        var segments = new List<string>();
        var current = expression;

        while (current != null)
        {
            segments.Insert(0, GetJsonPropertyName(current.Member.Name));

            if (current.Expression is ParameterExpression)
                break;

            current = current.Expression as MemberExpression;
        }

        return "$." + string.Join(".", segments);
    }

    private string GetJsonPropertyName(string clrPropertyName)
    {
        return _namingPolicy?.ConvertName(clrPropertyName) ?? clrPropertyName;
    }

    private static Type GetMemberType(MemberInfo member)
    {
        return member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new NotSupportedException($"Unsupported member type: {member.MemberType}")
        };
    }

    private static string? GetSqlType(Type type)
    {
        if (type == typeof(int)) return "int";
        if (type == typeof(long)) return "bigint";
        if (type == typeof(short)) return "smallint";
        if (type == typeof(double)) return "float";
        if (type == typeof(decimal)) return "decimal(18,6)";
        if (type == typeof(float)) return "real";
        if (type == typeof(Guid)) return "uniqueidentifier";
        if (type == typeof(DateTime)) return "datetime2";
        if (type == typeof(DateTimeOffset)) return "datetimeoffset";
        return null;
    }
}
