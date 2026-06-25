using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using JasperFx.Core.Reflection;
using Polecat.Serialization;
using Polecat.Storage;
using Weasel.Core;

namespace Polecat.Linq.Members;

/// <summary>
///     Resolves C# member expressions to IQueryableMember instances for SQL generation.
/// </summary>
internal class MemberFactory : IMemberResolver
{
    private readonly JsonNamingPolicy? _namingPolicy;
    private readonly EnumStorage _enumStorage;
    private readonly Type _idType;
    private readonly ValueTypeInfo? _valueTypeId;
    private readonly DocumentMapping _mapping;
    private readonly bool _useReturning;

    public MemberFactory(StoreOptions options, DocumentMapping mapping)
    {
        _enumStorage = options.Serializer.EnumStorage;
        _idType = mapping.IdType;
        _valueTypeId = mapping.ValueTypeId;
        _mapping = mapping;
        // #217: JSON_VALUE(... RETURNING <type>) is only available when the data column is the
        // native `json` type (SQL Server 2025+), which is exactly what UseNativeJsonType selects.
        // On nvarchar(max) storage the RETURNING clause is a syntax error, so fall back to CAST.
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
        // Check if it's the Id property on the root document
        if (expression.Member.Name == "Id" && expression.Expression is ParameterExpression)
        {
            return new IdMember(_idType, _valueTypeId);
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
            return new EnumMember(rawLocator, underlying, _enumStorage, _namingPolicy, jsonPath, _useReturning);
        }

        if (underlying == typeof(bool))
        {
            return new QueryableMember(rawLocator, rawLocator, memberType, isBoolean: true);
        }

        var sqlType = GetSqlType(underlying);
        var typedLocator = BuildTypedLocator(jsonPath, rawLocator, sqlType);

        // #223: if a Default-casing Index(...) covers this member, emit the EXACT computed-column
        // expression the index defines instead of the bare/uncast locator. SQL Server then matches
        // the predicate to the persisted computed column and can seek the index. Without this the
        // translator's expression (e.g. bare JSON_VALUE for a string, CAST(... AS datetimeoffset)
        // for a date) never lines up with the index column, so the index is dead weight.
        if (TryGetIndexedLocator(jsonPath, underlying, out var indexedLocator))
        {
            typedLocator = indexedLocator;
        }

        return new QueryableMember(rawLocator, typedLocator, memberType);
    }

    private bool TryGetIndexedLocator(string jsonPath, Type underlying, out string locator)
    {
        locator = string.Empty;

        // Only Default-casing indexes are predicate-transparent. Upper/Lower computed columns
        // fold case, so a plain equality/range predicate must not be rewritten onto them.
        var index = _mapping.Indexes.FirstOrDefault(i =>
            i.Casing == IndexCasing.Default && Array.IndexOf(i.JsonPaths, jsonPath) >= 0);
        if (index == null) return false;

        var sqlType = index.ResolveSqlType(jsonPath, underlying);
        locator = DocumentIndex.ComputedColumnExpression(jsonPath, sqlType, IndexCasing.Default);
        return true;
    }

    /// <summary>
    ///     #217: builds the typed locator, preferring JSON_VALUE(... RETURNING type) on native json
    ///     storage (SQL Server 2025+) over CAST(JSON_VALUE(...) AS type). RETURNING does not support
    ///     uniqueidentifier, so Guid members keep CAST. A null sqlType (string/bool) needs no typing.
    ///     Note: members covered by a Default-casing Index(...) are instead rewritten to the index's
    ///     computed-column expression in CreateMember (which takes precedence), so they keep matching
    ///     the persisted CAST/CONVERT column and stay seekable (#223).
    /// </summary>
    internal static string BuildTypedLocator(string jsonPath, string rawLocator, string? sqlType,
        bool useReturning)
    {
        if (sqlType == null) return rawLocator;
        return useReturning && SupportsReturning(sqlType)
            ? $"JSON_VALUE(data, '{jsonPath}' RETURNING {sqlType})"
            : $"CAST({rawLocator} AS {sqlType})";
    }

    private string BuildTypedLocator(string jsonPath, string rawLocator, string? sqlType)
        => BuildTypedLocator(jsonPath, rawLocator, sqlType, _useReturning);

    /// <summary>
    ///     RETURNING supports every type Polecat emits except uniqueidentifier (not in the SQL Server
    ///     JSON_VALUE RETURNING type list), so Guid members fall back to CAST.
    /// </summary>
    internal static bool SupportsReturning(string sqlType) => sqlType != "uniqueidentifier";

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
        return SqlTypeMap.For(type);
    }
}
