using System.Data.Common;
using Polecat.Metadata;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

internal class CheckExistsBatchQueryItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<bool> _tcs = new();
    private readonly object _id;
    private readonly DocumentProvider _provider;
    private readonly string _tenantId;

    public CheckExistsBatchQueryItem(object id, DocumentProvider provider, string tenantId)
    {
        _id = id;
        _provider = provider;
        _tenantId = tenantId;
    }

    public Task<bool> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        var softDeleteFilter = _provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        builder.Append($"SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM {_provider.Mapping.QualifiedTableName} WHERE id = ");
        builder.AppendParameter(_id);
        builder.Append(" AND tenant_id = ");
        builder.AppendParameter(_tenantId);
        builder.Append(softDeleteFilter);
        builder.Append(") THEN 1 ELSE 0 END AS BIT);\n");
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            _tcs.SetResult(reader.GetBoolean(0));
        }
        else
        {
            _tcs.SetResult(false);
        }
    }
}
