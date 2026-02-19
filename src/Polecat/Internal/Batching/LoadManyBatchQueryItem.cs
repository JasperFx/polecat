using Microsoft.Data.SqlClient;
using Polecat.Linq.SqlGeneration;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Internal.Batching;

internal class LoadManyBatchQueryItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<IReadOnlyList<T>> _tcs = new();
    private readonly object[] _ids;
    private readonly DocumentProvider _provider;
    private readonly ISerializer _serializer;
    private readonly string _tenantId;

    public LoadManyBatchQueryItem(object[] ids, DocumentProvider provider, ISerializer serializer, string tenantId)
    {
        _ids = ids;
        _provider = provider;
        _serializer = serializer;
        _tenantId = tenantId;
    }

    public Task<IReadOnlyList<T>> Result => _tcs.Task;

    public void WriteSql(CommandBuilder builder)
    {
        if (_ids.Length == 0)
        {
            builder.Append($"{_provider.SelectSql} WHERE 1 = 0;\n");
            return;
        }

        var idParams = new string[_ids.Length];
        for (var i = 0; i < _ids.Length; i++)
        {
            idParams[i] = builder.AddParameter(_ids[i]);
        }

        var tenantParam = builder.AddParameter(_tenantId);

        var softDeleteFilter = _provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        builder.Append($"{_provider.SelectSql} WHERE id IN ({string.Join(", ", idParams)}) AND tenant_id = {tenantParam}{softDeleteFilter};\n");
    }

    public async Task ReadResultSetAsync(SqlDataReader reader, CancellationToken token)
    {
        var results = new List<T>();
        while (await reader.ReadAsync(token))
        {
            var json = reader.GetString(1); // data column
            var doc = _serializer.FromJson<T>(json);
            QuerySession.SyncVersionProperties(doc, reader, _provider);
            results.Add(doc);
        }

        _tcs.SetResult(results);
    }
}
