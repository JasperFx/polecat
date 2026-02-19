using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Polecat.Serialization;
using Polecat.Storage;

namespace Polecat.Linq.Members;

/// <summary>
///     Resolves C# member expressions to IQueryableMember instances for SQL generation.
/// </summary>
internal class MemberFactory
{
    private readonly JsonNamingPolicy? _namingPolicy;
    private readonly EnumStorage _enumStorage;
    private readonly Type _idType;

    public MemberFactory(StoreOptions options, DocumentMapping mapping)
    {
        _enumStorage = options.Serializer.EnumStorage;
        _idType = mapping.IdType;

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
        // Check if it's the Id property on the root document
        if (expression.Member.Name == "Id" && expression.Expression is ParameterExpression)
        {
            return new IdMember(_idType);
        }

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
        return null; // string, bool, etc. — no CAST needed
    }
}
