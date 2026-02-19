using Microsoft.Data.SqlClient;
using Polecat.Linq.SqlGeneration;
using Polecat.Serialization;

namespace Polecat.Internal.Batching;

internal class QueryListBatchItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<IReadOnlyList<T>> _tcs = new();
    private readonly Statement _statement;
    private readonly ISerializer _serializer;

    public QueryListBatchItem(Statement statement, ISerializer serializer)
    {
        _statement = statement;
        _serializer = serializer;
    }

    public Task<IReadOnlyList<T>> Result => _tcs.Task;

    public void WriteSql(CommandBuilder builder)
    {
        _statement.Apply(builder);
        builder.Append(";\n");
    }

    public async Task ReadResultSetAsync(SqlDataReader reader, CancellationToken token)
    {
        var results = new List<T>();
        while (await reader.ReadAsync(token))
        {
            var json = reader.GetString(0); // data column
            var doc = _serializer.FromJson<T>(json);
            results.Add(doc);
        }

        _tcs.SetResult(results);
    }
}

internal class QueryCountBatchItem : IBatchQueryItem
{
    private readonly TaskCompletionSource<int> _tcs = new();
    private readonly Statement _statement;

    public QueryCountBatchItem(Statement statement)
    {
        _statement = statement;
        _statement.SelectColumns = "COUNT(*)";
    }

    public Task<int> Result => _tcs.Task;

    public void WriteSql(CommandBuilder builder)
    {
        _statement.Apply(builder);
        builder.Append(";\n");
    }

    public async Task ReadResultSetAsync(SqlDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            _tcs.SetResult(reader.GetInt32(0));
        }
        else
        {
            _tcs.SetResult(0);
        }
    }
}

internal class QueryAnyBatchItem : IBatchQueryItem
{
    private readonly TaskCompletionSource<bool> _tcs = new();
    private readonly Statement _statement;

    public QueryAnyBatchItem(Statement statement)
    {
        _statement = statement;
        _statement.SelectColumns = "1";
        _statement.Limit = 1;
        _statement.IsExistsWrapper = true;
    }

    public Task<bool> Result => _tcs.Task;

    public void WriteSql(CommandBuilder builder)
    {
        _statement.Apply(builder);
        builder.Append(";\n");
    }

    public async Task ReadResultSetAsync(SqlDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            _tcs.SetResult(reader.GetInt32(0) == 1);
        }
        else
        {
            _tcs.SetResult(false);
        }
    }
}

internal class QueryFirstOrDefaultBatchItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<T?> _tcs = new();
    private readonly Statement _statement;
    private readonly ISerializer _serializer;

    public QueryFirstOrDefaultBatchItem(Statement statement, ISerializer serializer)
    {
        _statement = statement;
        _serializer = serializer;
        _statement.Limit = 1;
    }

    public Task<T?> Result => _tcs.Task;

    public void WriteSql(CommandBuilder builder)
    {
        _statement.Apply(builder);
        builder.Append(";\n");
    }

    public async Task ReadResultSetAsync(SqlDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            var json = reader.GetString(0);
            _tcs.SetResult(_serializer.FromJson<T>(json));
        }
        else
        {
            _tcs.SetResult(null);
        }
    }
}
