using System.Data.Common;
using Polecat.Internal;
using Polecat.Serialization;
using Weasel.Core;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Patching;

/// <summary>
///     IStorageOperation that generates SQL UPDATE statements using JSON_MODIFY()
///     to patch document JSON data in-place.
/// </summary>
internal class PatchOperation : Polecat.Internal.IStorageOperation
{
    private readonly DocumentMapping _mapping;
    private readonly List<Action<ICommandBuilder>> _actions;
    private readonly Action<ICommandBuilder> _whereClauseWriter;

    public PatchOperation(DocumentMapping mapping, List<Action<ICommandBuilder>> actions,
        Action<ICommandBuilder> whereClauseWriter)
    {
        _mapping = mapping;
        _actions = actions;
        _whereClauseWriter = whereClauseWriter;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role() => OperationRole.Patch;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        foreach (var action in _actions)
        {
            builder.Append($"UPDATE {_mapping.QualifiedTableName} SET data = ");
            action(builder);
            builder.Append(", last_modified = SYSDATETIMEOFFSET() WHERE ");
            _whereClauseWriter(builder);
            builder.Append(";\n");
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;

    // --- Static helpers for building JSON_MODIFY expressions ---

    internal static Action<ICommandBuilder> SetScalar(string jsonPath, object? value)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', ");
            builder.AppendParameter(value ?? DBNull.Value);
            builder.Append(")");
        };
    }

    internal static Action<ICommandBuilder> SetComplex(string jsonPath, string jsonValue)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> IncrementInt(string jsonPath, object increment, string sqlType)
    {
        return builder =>
        {
            builder.Append(
                $"JSON_MODIFY(data, '$.{jsonPath}', CAST(JSON_VALUE(data, '$.{jsonPath}') AS {sqlType}) + ");
            builder.AppendParameter(increment);
            builder.Append(")");
        };
    }

    internal static Action<ICommandBuilder> IncrementFloat(string jsonPath, object increment, string sqlType)
    {
        return builder =>
        {
            builder.Append(
                $"JSON_MODIFY(data, '$.{jsonPath}', CAST(CAST(JSON_VALUE(data, '$.{jsonPath}') AS {sqlType}) + CAST(");
            builder.AppendParameter(increment);
            builder.Append($" AS {sqlType}) AS {sqlType}))");
        };
    }

    internal static Action<ICommandBuilder> AppendScalar(string jsonPath, object element)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', ");
            builder.AppendParameter(element);
            builder.Append(")");
        };
    }

    internal static Action<ICommandBuilder> AppendComplex(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY(");
            builder.AppendParameter(jsonElement);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> AppendIfNotExistsScalar(string jsonPath, object element)
    {
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = CAST(");
            builder.AppendParameter(element);
            var paramName = "@" + builder.LastParameterName!;
            builder.Append($" AS nvarchar(max))) ");
            builder.Append($"THEN JSON_MODIFY(data, 'append $.{jsonPath}', {paramName}) ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> AppendIfNotExistsComplex(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = ");
            builder.AppendParameter(jsonElement);
            var paramName = "@" + builder.LastParameterName!;
            builder.Append(") ");
            builder.Append($"THEN JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY({paramName})) ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> SetDictKey(string dictPath, string key, string jsonValue)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{dictPath}.{key}', JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> SetDictKeyIfNotExists(string dictPath, string key, string jsonValue)
    {
        return builder =>
        {
            builder.Append($"CASE WHEN JSON_VALUE(data, '$.{dictPath}.{key}') IS NULL AND ");
            builder.Append($"JSON_QUERY(data, '$.{dictPath}.{key}') IS NULL ");
            builder.Append($"THEN JSON_MODIFY(data, '$.{dictPath}.{key}', JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append(")) ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> InsertAtEnd(string jsonPath, object element, bool isComplex,
        string? jsonElement)
    {
        return isComplex ? AppendComplex(jsonPath, jsonElement!) : AppendScalar(jsonPath, element);
    }

    internal static Action<ICommandBuilder> InsertAtIndex(string jsonPath, int index, object element, bool isComplex,
        string? jsonElement, ISerializer serializer)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                "SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
            builder.Append("FROM (");
            builder.Append(
                $"SELECT j.value as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("UNION ALL ");
            builder.Append("SELECT ");
            builder.AppendParameter(isComplex ? jsonElement! : serializer.ToJson(element));
            builder.Append($", {index * 2}");
            builder.Append(") t");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> InsertIfNotExistsScalar(string jsonPath, object element, int? index,
        ISerializer serializer)
    {
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = CAST(");
            builder.AppendParameter(element);
            var valParam = "@" + builder.LastParameterName!;
            builder.Append(" AS nvarchar(max))) THEN ");

            if (index.HasValue)
            {
                builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
                builder.Append("COALESCE(");
                builder.Append("'[' + (");
                builder.Append(
                    "SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
                builder.Append("FROM (");
                builder.Append(
                    $"SELECT j.value as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
                builder.Append("UNION ALL ");
                builder.Append("SELECT ");
                builder.AppendParameter(serializer.ToJson(element));
                builder.Append($", {index.Value * 2}");
                builder.Append(") t");
                builder.Append(") + ']', '[]')");
                builder.Append("))");
            }
            else
            {
                builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', {valParam})");
            }

            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> InsertIfNotExistsComplex(string jsonPath, string jsonElement, int? index,
        ISerializer serializer)
    {
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = ");
            builder.AppendParameter(jsonElement);
            var paramName = "@" + builder.LastParameterName!;
            builder.Append(") THEN ");

            if (index.HasValue)
            {
                builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
                builder.Append("COALESCE(");
                builder.Append("'[' + (");
                builder.Append(
                    "SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
                builder.Append("FROM (");
                builder.Append(
                    $"SELECT j.value as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
                builder.Append("UNION ALL ");
                builder.Append($"SELECT {paramName}, {index.Value * 2}");
                builder.Append(") t");
                builder.Append(") + ']', '[]')");
                builder.Append("))");
            }
            else
            {
                builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY({paramName}))");
            }

            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> RemoveScalarFirst(string jsonPath, object element)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                "SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE CAST(j.[key] AS int) != (");
            builder.Append(
                $"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j2 ");
            builder.Append("WHERE j2.value = CAST(");
            builder.AppendParameter(element);
            builder.Append(" AS nvarchar(max))");
            builder.Append(")");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveScalarAll(string jsonPath, object element)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                "SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE j.value != CAST(");
            builder.AppendParameter(element);
            builder.Append(" AS nvarchar(max))");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveComplexFirst(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                "SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE CAST(j.[key] AS int) != (");
            builder.Append(
                $"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j2 ");
            builder.Append("WHERE j2.value = ");
            builder.AppendParameter(jsonElement);
            builder.Append(")");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveComplexAll(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                "SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE j.value != ");
            builder.AppendParameter(jsonElement);
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveDictKey(string dictPath, string key)
    {
        return builder => { builder.Append($"JSON_MODIFY(data, '$.{dictPath}.{key}', NULL)"); };
    }

    internal static Action<ICommandBuilder> DeleteProperty(string jsonPath)
    {
        return builder => { builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', NULL)"); };
    }

    internal static Action<ICommandBuilder> RenameProperty(string oldJsonPath, string newJsonPath, bool isScalarType)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(JSON_MODIFY(data, '$.{newJsonPath}', ");
            var jsonFunc = isScalarType ? "JSON_VALUE" : "JSON_QUERY";
            builder.Append($"{jsonFunc}(data, '$.{oldJsonPath}')), ");
            builder.Append($"'$.{oldJsonPath}', NULL)");
        };
    }

    internal static Action<ICommandBuilder> DuplicateProperty(string sourcePath, string[] destPaths, bool isScalarType)
    {
        return builder =>
        {
            for (var i = 0; i < destPaths.Length; i++)
            {
                builder.Append("JSON_MODIFY(");
            }

            builder.Append("data");

            for (var i = 0; i < destPaths.Length; i++)
            {
                builder.Append($", '$.{destPaths[i]}', ");

                var jsonFunc = isScalarType ? "JSON_VALUE" : "JSON_QUERY";
                builder.Append($"{jsonFunc}(data, '$.{sourcePath}'))");
            }
        };
    }

    internal static bool IsScalarType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsPrimitive || underlying == typeof(string) || underlying == typeof(decimal) ||
               underlying == typeof(Guid) || underlying == typeof(DateTime) ||
               underlying == typeof(DateTimeOffset) ||
               underlying.IsEnum;
    }
}
