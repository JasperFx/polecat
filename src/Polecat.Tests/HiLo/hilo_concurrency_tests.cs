using Polecat.Tests.Harness;

namespace Polecat.Tests.HiLo;

/// <summary>
///     Stress tests for HiLo ID generation under concurrent load.
/// </summary>
public class hilo_concurrency_tests : OneOffConfigurationsContext
{
    const int Count = 10;
    
    public hilo_concurrency_tests()
    {
        ThreadPool.SetMinThreads(200, 200);
    }

    [Theory]
    [Repeat(Count)]
    public async Task concurrent_sessions_get_unique_int_ids(int _)
    {
        const int concurrency = 10;
        const int docsPerTask = 5;

        var allIds = new System.Collections.Concurrent.ConcurrentBag<int>();

        var tasks = Enumerable.Range(0, concurrency).Select(async _ =>
        {
            await using var session = theStore.LightweightSession();
            var docs = Enumerable.Range(0, docsPerTask)
                .Select(_ => new IntDoc { Name = $"Concurrent-{Guid.NewGuid():N}" })
                .ToList();

            foreach (var doc in docs)
            {
                session.Store(doc);
            }

            await session.SaveChangesAsync();

            foreach (var doc in docs)
            {
                allIds.Add(doc.Id);
            }
        }).ToList();

        await Task.WhenAll(tasks);

        var idList = allIds.ToList();
        idList.Count.ShouldBe(concurrency * docsPerTask);

        // All IDs must be unique
        idList.Distinct().Count().ShouldBe(idList.Count);

        // All IDs must be positive
        idList.ShouldAllBe(id => id > 0);
    }

    [Theory]
    [Repeat(Count)]
    public async Task concurrent_sessions_get_unique_long_ids(int _)
    {
        const int concurrency = 10;
        const int docsPerTask = 5;

        var allIds = new System.Collections.Concurrent.ConcurrentBag<long>();

        var tasks = Enumerable.Range(0, concurrency).Select(async _ =>
        {
            await using var session = theStore.LightweightSession();
            var docs = Enumerable.Range(0, docsPerTask)
                .Select(_ => new LongDoc { Name = $"Concurrent-{Guid.NewGuid():N}" })
                .ToList();

            foreach (var doc in docs)
            {
                session.Store(doc);
            }

            await session.SaveChangesAsync();

            foreach (var doc in docs)
            {
                allIds.Add(doc.Id);
            }
        }).ToList();

        await Task.WhenAll(tasks);

        var idList = allIds.ToList();
        idList.Count.ShouldBe(concurrency * docsPerTask);
        idList.Distinct().Count().ShouldBe(idList.Count);
        idList.ShouldAllBe(id => id > 0);
    }

    [Theory]
    [Repeat(Count)]
    public async Task concurrent_bulk_inserts_get_unique_ids(int _)
    {
        const int concurrency = 5;
        const int docsPerBatch = 10;

        var allIds = new System.Collections.Concurrent.ConcurrentBag<int>();

        var tasks = Enumerable.Range(0, concurrency).Select(async _ =>
        {
            var docs = Enumerable.Range(0, docsPerBatch)
                .Select(_ => new IntDoc { Name = $"BulkConcurrent-{Guid.NewGuid():N}" })
                .ToList();

            await theStore.Advanced.BulkInsertAsync(docs);

            foreach (var doc in docs)
            {
                allIds.Add(doc.Id);
            }
        }).ToList();

        await Task.WhenAll(tasks);

        var idList = allIds.ToList();
        idList.Count.ShouldBe(concurrency * docsPerBatch);
        idList.Distinct().Count().ShouldBe(idList.Count);
        idList.ShouldAllBe(id => id > 0);
    }

    [Theory]
    [Repeat(Count)]
    public async Task sequential_hilo_ids_are_monotonically_increasing_within_session(int _)
    {
        await using var session = theStore.LightweightSession();
        var docs = new List<IntDoc>();

        for (var i = 0; i < 10; i++)
        {
            var doc = new IntDoc { Name = $"Sequential{i}" };
            session.Store(doc);
            docs.Add(doc);
        }

        await session.SaveChangesAsync();

        // Within a single session, IDs should be monotonically increasing
        for (var i = 1; i < docs.Count; i++)
        {
            docs[i].Id.ShouldBeGreaterThan(docs[i - 1].Id);
        }
    }
}
