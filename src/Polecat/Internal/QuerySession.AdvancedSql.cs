using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

internal partial class QuerySession : IAdvancedSql
{
    private const char DefaultPlaceholder = '?';

    public IAdvancedSql AdvancedSql => this;

    // ── Single type ────────────────────────────────────────────────────

    Task<IReadOnlyList<T>> IAdvancedSql.QueryAsync<T>(string sql, CancellationToken token, params object[] parameters)
        => ((IAdvancedSql)this).QueryAsync<T>(DefaultPlaceholder, sql, token, parameters);

    async Task<IReadOnlyList<T>> IAdvancedSql.QueryAsync<T>(char placeholder, string sql, CancellationToken token, params object[] parameters)
    {
        var (commandText, sqlParams) = PrepareCommand(sql, placeholder, parameters);
        var reader1 = AdvancedSqlResultReader.ForType(typeof(T), Serializer, _providers);

        await using var cmd = new SqlCommand(commandText);
        foreach (var p in sqlParams) cmd.Parameters.Add(p);

        Logger.OnBeforeExecute(commandText);
        await using var dbReader = await ExecuteReaderAsync(cmd, token);

        var list = new List<T>();
        while (await dbReader.ReadAsync(token))
        {
            var value = reader1.ReadValue(dbReader, 0);
            list.Add(value == null ? default! : (T)Convert.ChangeType(value, typeof(T)));
        }

        Logger.LogSuccess(commandText);
        return list;
    }

    // ── Two types ──────────────────────────────────────────────────────

    Task<IReadOnlyList<(T1, T2)>> IAdvancedSql.QueryAsync<T1, T2>(string sql, CancellationToken token, params object[] parameters)
        => ((IAdvancedSql)this).QueryAsync<T1, T2>(DefaultPlaceholder, sql, token, parameters);

    async Task<IReadOnlyList<(T1, T2)>> IAdvancedSql.QueryAsync<T1, T2>(char placeholder, string sql, CancellationToken token, params object[] parameters)
    {
        var (commandText, sqlParams) = PrepareCommand(sql, placeholder, parameters);
        var reader1 = AdvancedSqlResultReader.ForType(typeof(T1), Serializer, _providers);
        var reader2 = AdvancedSqlResultReader.ForType(typeof(T2), Serializer, _providers);

        await using var cmd = new SqlCommand(commandText);
        foreach (var p in sqlParams) cmd.Parameters.Add(p);

        Logger.OnBeforeExecute(commandText);
        await using var dbReader = await ExecuteReaderAsync(cmd, token);

        var list = new List<(T1, T2)>();
        while (await dbReader.ReadAsync(token))
        {
            var v1 = CastResult<T1>(reader1.ReadValue(dbReader, 0));
            var v2 = CastResult<T2>(reader2.ReadValue(dbReader, reader1.ColumnCount));
            list.Add((v1, v2));
        }

        Logger.LogSuccess(commandText);
        return list;
    }

    // ── Three types ────────────────────────────────────────────────────

    Task<IReadOnlyList<(T1, T2, T3)>> IAdvancedSql.QueryAsync<T1, T2, T3>(string sql, CancellationToken token, params object[] parameters)
        => ((IAdvancedSql)this).QueryAsync<T1, T2, T3>(DefaultPlaceholder, sql, token, parameters);

    async Task<IReadOnlyList<(T1, T2, T3)>> IAdvancedSql.QueryAsync<T1, T2, T3>(char placeholder, string sql, CancellationToken token, params object[] parameters)
    {
        var (commandText, sqlParams) = PrepareCommand(sql, placeholder, parameters);
        var reader1 = AdvancedSqlResultReader.ForType(typeof(T1), Serializer, _providers);
        var reader2 = AdvancedSqlResultReader.ForType(typeof(T2), Serializer, _providers);
        var reader3 = AdvancedSqlResultReader.ForType(typeof(T3), Serializer, _providers);

        await using var cmd = new SqlCommand(commandText);
        foreach (var p in sqlParams) cmd.Parameters.Add(p);

        Logger.OnBeforeExecute(commandText);
        await using var dbReader = await ExecuteReaderAsync(cmd, token);

        var list = new List<(T1, T2, T3)>();
        while (await dbReader.ReadAsync(token))
        {
            var offset = 0;
            var v1 = CastResult<T1>(reader1.ReadValue(dbReader, offset));
            offset += reader1.ColumnCount;
            var v2 = CastResult<T2>(reader2.ReadValue(dbReader, offset));
            offset += reader2.ColumnCount;
            var v3 = CastResult<T3>(reader3.ReadValue(dbReader, offset));
            list.Add((v1, v2, v3));
        }

        Logger.LogSuccess(commandText);
        return list;
    }

    // ── Streaming ──────────────────────────────────────────────────────

    IAsyncEnumerable<T> IAdvancedSql.StreamAsync<T>(string sql, CancellationToken token, params object[] parameters)
        => ((IAdvancedSql)this).StreamAsync<T>(DefaultPlaceholder, sql, token, parameters);

    async IAsyncEnumerable<T> IAdvancedSql.StreamAsync<T>(char placeholder, string sql, [EnumeratorCancellation] CancellationToken token, params object[] parameters)
    {
        var (commandText, sqlParams) = PrepareCommand(sql, placeholder, parameters);
        var reader1 = AdvancedSqlResultReader.ForType(typeof(T), Serializer, _providers);

        await using var cmd = new SqlCommand(commandText);
        foreach (var p in sqlParams) cmd.Parameters.Add(p);

        Logger.OnBeforeExecute(commandText);
        await using var dbReader = await ExecuteReaderAsync(cmd, token);

        while (await dbReader.ReadAsync(token))
        {
            var value = reader1.ReadValue(dbReader, 0);
            yield return value == null ? default! : (T)Convert.ChangeType(value, typeof(T));
        }

        Logger.LogSuccess(commandText);
    }

    IAsyncEnumerable<(T1, T2)> IAdvancedSql.StreamAsync<T1, T2>(string sql, CancellationToken token, params object[] parameters)
        => ((IAdvancedSql)this).StreamAsync<T1, T2>(DefaultPlaceholder, sql, token, parameters);

    async IAsyncEnumerable<(T1, T2)> IAdvancedSql.StreamAsync<T1, T2>(char placeholder, string sql, [EnumeratorCancellation] CancellationToken token, params object[] parameters)
    {
        var (commandText, sqlParams) = PrepareCommand(sql, placeholder, parameters);
        var reader1 = AdvancedSqlResultReader.ForType(typeof(T1), Serializer, _providers);
        var reader2 = AdvancedSqlResultReader.ForType(typeof(T2), Serializer, _providers);

        await using var cmd = new SqlCommand(commandText);
        foreach (var p in sqlParams) cmd.Parameters.Add(p);

        Logger.OnBeforeExecute(commandText);
        await using var dbReader = await ExecuteReaderAsync(cmd, token);

        while (await dbReader.ReadAsync(token))
        {
            var v1 = CastResult<T1>(reader1.ReadValue(dbReader, 0));
            var v2 = CastResult<T2>(reader2.ReadValue(dbReader, reader1.ColumnCount));
            yield return (v1, v2);
        }

        Logger.LogSuccess(commandText);
    }

    IAsyncEnumerable<(T1, T2, T3)> IAdvancedSql.StreamAsync<T1, T2, T3>(string sql, CancellationToken token, params object[] parameters)
        => ((IAdvancedSql)this).StreamAsync<T1, T2, T3>(DefaultPlaceholder, sql, token, parameters);

    async IAsyncEnumerable<(T1, T2, T3)> IAdvancedSql.StreamAsync<T1, T2, T3>(char placeholder, string sql, [EnumeratorCancellation] CancellationToken token, params object[] parameters)
    {
        var (commandText, sqlParams) = PrepareCommand(sql, placeholder, parameters);
        var reader1 = AdvancedSqlResultReader.ForType(typeof(T1), Serializer, _providers);
        var reader2 = AdvancedSqlResultReader.ForType(typeof(T2), Serializer, _providers);
        var reader3 = AdvancedSqlResultReader.ForType(typeof(T3), Serializer, _providers);

        await using var cmd = new SqlCommand(commandText);
        foreach (var p in sqlParams) cmd.Parameters.Add(p);

        Logger.OnBeforeExecute(commandText);
        await using var dbReader = await ExecuteReaderAsync(cmd, token);

        while (await dbReader.ReadAsync(token))
        {
            var offset = 0;
            var v1 = CastResult<T1>(reader1.ReadValue(dbReader, offset));
            offset += reader1.ColumnCount;
            var v2 = CastResult<T2>(reader2.ReadValue(dbReader, offset));
            offset += reader2.ColumnCount;
            var v3 = CastResult<T3>(reader3.ReadValue(dbReader, offset));
            yield return (v1, v2, v3);
        }

        Logger.LogSuccess(commandText);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static (string CommandText, SqlParameter[] Parameters) PrepareCommand(
        string sql, char placeholder, object[] parameters)
    {
        var sqlParams = new SqlParameter[parameters.Length];
        var commandText = sql.TrimStart();

        for (var i = 0; i < parameters.Length; i++)
        {
            var paramName = $"@p{i}";
            sqlParams[i] = parameters[i] == null
                ? new SqlParameter(paramName, DBNull.Value)
                : new SqlParameter(paramName, parameters[i]);

            // Replace first occurrence of placeholder with parameter name
            var idx = commandText.IndexOf(placeholder);
            if (idx < 0)
            {
                throw new InvalidOperationException(
                    $"Wrong number of supplied parameters. Expected at least {i + 1} placeholder(s) '{placeholder}' but found {i}.");
            }
            commandText = commandText[..idx] + paramName + commandText[(idx + 1)..];
        }

        return (commandText, sqlParams);
    }

    private static T CastResult<T>(object? value)
    {
        if (value == null) return default!;
        if (value is T typed) return typed;
        return (T)Convert.ChangeType(value, typeof(T));
    }
}
