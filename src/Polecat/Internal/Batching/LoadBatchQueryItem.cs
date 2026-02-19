using Microsoft.Data.SqlClient;
using Polecat.Linq.SqlGeneration;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Internal.Batching;

internal class LoadBatchQueryItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<T?> _tcs = new();
    private readonly object _id;
    private readonly DocumentProvider _provider;
    private readonly ISerializer _serializer;
    private readonly string _tenantId;

    public LoadBatchQueryItem(object id, DocumentProvider provider, ISerializer serializer, string tenantId)
    {
        _id = id;
        _provider = provider;
        _serializer = serializer;
        _tenantId = tenantId;
    }

    public Task<T?> Result => _tcs.Task;

    public void WriteSql(CommandBuilder builder)
    {
        var idParam = builder.AddParameter(_id);
        var tenantParam = builder.AddParameter(_tenantId);

        var softDeleteFilter = _provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        builder.Append($"{_provider.SelectSql} WHERE id = {idParam} AND tenant_id = {tenantParam}{softDeleteFilter};\n");
    }

    public async Task ReadResultSetAsync(SqlDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            var json = reader.GetString(1); // data column
            var doc = _serializer.FromJson<T>(json);
            QuerySession.SyncVersionProperties(doc, reader, _provider);
            _tcs.SetResult(doc);
        }
        else
        {
            _tcs.SetResult(null);
        }
    }
}
