using Polecat.Linq;
using Polecat.Linq.Metadata;
using Polecat.Tests.Harness;
using Polecat.Tests.Linq;

namespace Polecat.Tests.Metadata;

[Collection("integration")]
public class modified_since_and_before_queries : IntegrationContext
{
    public modified_since_and_before_queries(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task modified_since_filters_documents()
    {
        var doc1 = new LinqTarget { Id = Guid.NewGuid(), Name = "old-doc" };

        theSession.Store(doc1);
        await theSession.SaveChangesAsync();

        // Small delay so second doc has a later last_modified
        await Task.Delay(50);
        var cutoff = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        var doc2 = new LinqTarget { Id = Guid.NewGuid(), Name = "new-doc" };
        await using var session2 = theStore.LightweightSession();
        session2.Store(doc2);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .ModifiedSince(cutoff)
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc2.Id);
    }

    [Fact]
    public async Task modified_before_filters_documents()
    {
        var doc1 = new LinqTarget { Id = Guid.NewGuid(), Name = "early-doc" };

        theSession.Store(doc1);
        await theSession.SaveChangesAsync();

        // Small delay so second doc has a later last_modified
        await Task.Delay(50);
        var cutoff = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        var doc2 = new LinqTarget { Id = Guid.NewGuid(), Name = "late-doc" };
        await using var session2 = theStore.LightweightSession();
        session2.Store(doc2);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .ModifiedBefore(cutoff)
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc1.Id);
    }

    [Fact]
    public async Task modified_since_and_before_combined()
    {
        var doc1 = new LinqTarget { Id = Guid.NewGuid(), Name = "before-window" };
        theSession.Store(doc1);
        await theSession.SaveChangesAsync();

        await Task.Delay(50);
        var start = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        var doc2 = new LinqTarget { Id = Guid.NewGuid(), Name = "in-window" };
        await using var session2 = theStore.LightweightSession();
        session2.Store(doc2);
        await session2.SaveChangesAsync();

        await Task.Delay(50);
        var end = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        var doc3 = new LinqTarget { Id = Guid.NewGuid(), Name = "after-window" };
        await using var session3 = theStore.LightweightSession();
        session3.Store(doc3);
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .ModifiedSince(start)
            .ModifiedBefore(end)
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id || x.Id == doc3.Id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc2.Id);
    }

    [Fact]
    public async Task modified_since_with_count()
    {
        var doc1 = new LinqTarget { Id = Guid.NewGuid(), Name = "old-count" };
        theSession.Store(doc1);
        await theSession.SaveChangesAsync();

        await Task.Delay(50);
        var cutoff = DateTimeOffset.UtcNow;
        await Task.Delay(50);

        var doc2 = new LinqTarget { Id = Guid.NewGuid(), Name = "new-count-a" };
        var doc3 = new LinqTarget { Id = Guid.NewGuid(), Name = "new-count-b" };
        await using var session2 = theStore.LightweightSession();
        session2.Store(doc2, doc3);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Query<LinqTarget>()
            .ModifiedSince(cutoff)
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id || x.Id == doc3.Id)
            .CountAsync();

        count.ShouldBe(2);
    }
}
