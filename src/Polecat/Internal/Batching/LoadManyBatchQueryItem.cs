using System.Data.Common;
using Polecat.Storage.ClosedShape;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Internal.Batching;

/// <summary>
///     Batched multi-document load. Composes the closed-shape whole-document SELECT
///     (#273 doc-side convergence) into the shared <c>SqlBatch</c> command via the QueryOnly
///     storage and materializes rows through the closed-shape selector.
/// </summary>
internal class LoadManyBatchQueryItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<IReadOnlyList<T>> _tcs = new();
    private readonly object[] _ids;
    private readonly IPolecatBatchLoadStorage<T> _storage;
    private readonly IStorageSession _session;
    private readonly string _tenantId;

    public LoadManyBatchQueryItem(object[] ids, IPolecatBatchLoadStorage<T> storage, IStorageSession session,
        string tenantId)
    {
        _ids = ids;
        _storage = storage;
        _session = session;
        _tenantId = tenantId;
    }

    public Task<IReadOnlyList<T>> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder) => _storage.WriteLoadManySql(builder, _ids, _tenantId);

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        var selector = _storage.BuildLoadSelector(_session);
        var results = new List<T>();
        while (await reader.ReadAsync(token))
        {
            results.Add(await selector.ResolveAsync(reader, token));
        }

        _tcs.SetResult(results);
    }
}
