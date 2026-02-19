namespace Polecat.Batching;

/// <summary>
///     Batches multiple Load/Query operations into a single database roundtrip
///     using SQL Server's multiple result sets.
/// </summary>
public interface IBatchedQuery
{
    Task<T?> Load<T>(Guid id) where T : class;
    Task<T?> Load<T>(string id) where T : class;
    Task<T?> Load<T>(int id) where T : class;
    Task<T?> Load<T>(long id) where T : class;

    Task<IReadOnlyList<T>> LoadMany<T>(params Guid[] ids) where T : class;
    Task<IReadOnlyList<T>> LoadMany<T>(params string[] ids) where T : class;

    IBatchedQueryable<T> Query<T>() where T : class;

    Task Execute(CancellationToken token = default);
}
