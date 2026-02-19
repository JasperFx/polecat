using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq.SqlGeneration;
using Polecat.Serialization;
using Polecat.Storage;

namespace Polecat.Patching;

/// <summary>
///     IStorageOperation that generates SQL UPDATE statements using JSON_MODIFY()
///     to patch document JSON data in-place.
/// </summary>
internal class PatchOperation : IStorageOperation
{
    private readonly DocumentMapping _mapping;
    private readonly List<Action<CommandBuilder>> _actions;
    private readonly Action<CommandBuilder> _whereClauseWriter;

    public PatchOperation(DocumentMapping mapping, List<Action<CommandBuilder>> actions,
        Action<CommandBuilder> whereClauseWriter)
    {
        _mapping = mapping;
        _actions = actions;
        _whereClauseWriter = whereClauseWriter;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Patch;

    public void ConfigureCommand(SqlCommand command)
    {
        var builder = new CommandBuilder();
        foreach (var action in _actions)
        {
            builder.Append($"UPDATE {_mapping.QualifiedTableName} SET data = ");
            action(builder);
            builder.Append(", last_modified = SYSDATETIMEOFFSET() WHERE ");
            _whereClauseWriter(builder);
            builder.Append(";\n");
        }

        builder.ApplyTo(command);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }

    // --- Static helpers for building JSON_MODIFY expressions ---

    internal static Action<CommandBuilder> SetScalar(string jsonPath, object? value)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', ");
            builder.AppendParameter(value ?? DBNull.Value);
            builder.Append(")");
        };
    }

    internal static Action<CommandBuilder> SetComplex(string jsonPath, string jsonValue)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> IncrementInt(string jsonPath, object increment, string sqlType)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', CAST(JSON_VALUE(data, '$.{jsonPath}') AS {sqlType}) + ");
            builder.AppendParameter(increment);
            builder.Append(")");
        };
    }

    internal static Action<CommandBuilder> IncrementFloat(string jsonPath, object increment, string sqlType)
    {
        return builder =>
        {
            // Cast both sides to float for consistent arithmetic, then cast result
            builder.Append(
                $"JSON_MODIFY(data, '$.{jsonPath}', CAST(CAST(JSON_VALUE(data, '$.{jsonPath}') AS {sqlType}) + CAST(");
            builder.AppendParameter(increment);
            builder.Append($" AS {sqlType}) AS {sqlType}))");
        };
    }

    internal static Action<CommandBuilder> AppendScalar(string jsonPath, object element)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', ");
            builder.AppendParameter(element);
            builder.Append(")");
        };
    }

    internal static Action<CommandBuilder> AppendComplex(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY(");
            builder.AppendParameter(jsonElement);
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> AppendIfNotExistsScalar(string jsonPath, object element)
    {
        return builder =>
        {
            // Conditional: only append if element doesn't exist in array
            var paramName = builder.AddParameter(element);
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append($"WHERE value = CAST({paramName} AS nvarchar(max))) ");
            builder.Append($"THEN JSON_MODIFY(data, 'append $.{jsonPath}', {paramName}) ELSE data END");
        };
    }

    internal static Action<CommandBuilder> AppendIfNotExistsComplex(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            // Compare full JSON text for complex objects
            var paramName = builder.AddParameter(jsonElement);
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append($"WHERE value = {paramName}) ");
            builder.Append($"THEN JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY({paramName})) ELSE data END");
        };
    }

    internal static Action<CommandBuilder> SetDictKey(string dictPath, string key, string jsonValue)
    {
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{dictPath}.{key}', JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> SetDictKeyIfNotExists(string dictPath, string key, string jsonValue)
    {
        return builder =>
        {
            var paramName = builder.AddParameter(jsonValue);
            builder.Append($"CASE WHEN JSON_VALUE(data, '$.{dictPath}.{key}') IS NULL AND ");
            builder.Append($"JSON_QUERY(data, '$.{dictPath}.{key}') IS NULL ");
            builder.Append(
                $"THEN JSON_MODIFY(data, '$.{dictPath}.{key}', JSON_QUERY({paramName})) ELSE data END");
        };
    }

    internal static Action<CommandBuilder> InsertAtEnd(string jsonPath, object element, bool isComplex,
        string? jsonElement)
    {
        // Insert at end = same as Append
        return isComplex ? AppendComplex(jsonPath, jsonElement!) : AppendScalar(jsonPath, element);
    }

    internal static Action<CommandBuilder> InsertAtIndex(string jsonPath, int index, object element, bool isComplex,
        string? jsonElement, ISerializer serializer)
    {
        return builder =>
        {
            // Rebuild array with element inserted at the given index
            var paramName = isComplex
                ? builder.AddParameter(jsonElement!)
                : builder.AddParameter(serializer.ToJson(element));

            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append($"SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
            builder.Append("FROM (");
            builder.Append(
                $"SELECT j.value as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("UNION ALL ");
            builder.Append($"SELECT {paramName}, {index * 2}");
            builder.Append(") t");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> InsertIfNotExistsScalar(string jsonPath, object element, int? index,
        ISerializer serializer)
    {
        return builder =>
        {
            var valParam = builder.AddParameter(element);

            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append($"WHERE value = CAST({valParam} AS nvarchar(max))) THEN ");

            if (index.HasValue)
            {
                var jsonParam = builder.AddParameter(serializer.ToJson(element));
                builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
                builder.Append("COALESCE(");
                builder.Append("'[' + (");
                builder.Append(
                    $"SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
                builder.Append("FROM (");
                builder.Append(
                    $"SELECT j.value as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
                builder.Append("UNION ALL ");
                builder.Append($"SELECT {jsonParam}, {index.Value * 2}");
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

    internal static Action<CommandBuilder> InsertIfNotExistsComplex(string jsonPath, string jsonElement, int? index,
        ISerializer serializer)
    {
        return builder =>
        {
            var paramName = builder.AddParameter(jsonElement);

            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append($"WHERE value = {paramName}) THEN ");

            if (index.HasValue)
            {
                builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
                builder.Append("COALESCE(");
                builder.Append("'[' + (");
                builder.Append(
                    $"SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
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

    internal static Action<CommandBuilder> RemoveScalarFirst(string jsonPath, object element)
    {
        return builder =>
        {
            var paramName = builder.AddParameter(element);
            // Remove only the first occurrence by excluding the minimum key that matches
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append($"WHERE CAST(j.[key] AS int) != (");
            builder.Append(
                $"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j2 ");
            builder.Append($"WHERE j2.value = CAST({paramName} AS nvarchar(max))");
            builder.Append(")");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> RemoveScalarAll(string jsonPath, object element)
    {
        return builder =>
        {
            var paramName = builder.AddParameter(element);
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append($"WHERE j.value != CAST({paramName} AS nvarchar(max))");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> RemoveComplexFirst(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            var paramName = builder.AddParameter(jsonElement);
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append($"WHERE CAST(j.[key] AS int) != (");
            builder.Append(
                $"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j2 ");
            builder.Append($"WHERE j2.value = {paramName}");
            builder.Append(")");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> RemoveComplexAll(string jsonPath, string jsonElement)
    {
        return builder =>
        {
            var paramName = builder.AddParameter(jsonElement);
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG(j.value, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append($"WHERE j.value != {paramName}");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<CommandBuilder> RemoveDictKey(string dictPath, string key)
    {
        return builder => { builder.Append($"JSON_MODIFY(data, '$.{dictPath}.{key}', NULL)"); };
    }

    internal static Action<CommandBuilder> DeleteProperty(string jsonPath)
    {
        return builder => { builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', NULL)"); };
    }

    internal static Action<CommandBuilder> RenameProperty(string oldJsonPath, string newJsonPath)
    {
        return builder =>
        {
            // Read old value (try JSON_QUERY first for complex, fall back to JSON_VALUE for scalar)
            builder.Append($"JSON_MODIFY(JSON_MODIFY(data, '$.{newJsonPath}', ");
            builder.Append($"COALESCE(JSON_QUERY(data, '$.{oldJsonPath}'), JSON_VALUE(data, '$.{oldJsonPath}'))), ");
            builder.Append($"'$.{oldJsonPath}', NULL)");
        };
    }

    internal static Action<CommandBuilder> DuplicateProperty(string sourcePath, string[] destPaths)
    {
        return builder =>
        {
            // Build nested JSON_MODIFY: innermost reads source, each level sets a destination
            // Start from the innermost
            for (var i = 0; i < destPaths.Length; i++)
            {
                builder.Append($"JSON_MODIFY(");
            }

            builder.Append("data");

            for (var i = 0; i < destPaths.Length; i++)
            {
                builder.Append($", '$.{destPaths[i]}', ");
                builder.Append(
                    $"COALESCE(JSON_QUERY(data, '$.{sourcePath}'), JSON_VALUE(data, '$.{sourcePath}')))");
            }
        };
    }

    internal static bool IsScalarType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsPrimitive || underlying == typeof(string) || underlying == typeof(decimal) ||
               underlying == typeof(Guid) || underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset) ||
               underlying.IsEnum;
    }
}
