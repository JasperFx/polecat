using System.Data.Common;
using Polecat.Storage.ClosedShape;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Internal.Batching;

/// <summary>
///     Batched single-document load. Composes the closed-shape whole-document SELECT
///     (#273 doc-side convergence) into the shared <c>SqlBatch</c> command via the QueryOnly
///     storage and materializes the row through the closed-shape selector — the same read
///     path as <c>session.Query&lt;T&gt;()</c>.
/// </summary>
internal class LoadBatchQueryItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<T?> _tcs = new();
    private readonly object _id;
    private readonly IPolecatBatchLoadStorage<T> _storage;
    private readonly IStorageSession _session;
    private readonly string _tenantId;

    public LoadBatchQueryItem(object id, IPolecatBatchLoadStorage<T> storage, IStorageSession session, string tenantId)
    {
        _id = id;
        _storage = storage;
        _session = session;
        _tenantId = tenantId;
    }

    public Task<T?> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder) => _storage.WriteLoadByIdSql(builder, _id, _tenantId);

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            var selector = _storage.BuildLoadSelector(_session);
            var doc = await selector.ResolveAsync(reader, token);
            _tcs.SetResult(doc);
        }
        else
        {
            _tcs.SetResult(null);
        }
    }
}
