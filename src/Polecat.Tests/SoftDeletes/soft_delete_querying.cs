using Polecat.Linq;
using Polecat.Linq.SoftDeletes;
using Polecat.Tests.Harness;

namespace Polecat.Tests.SoftDeletes;

[Collection("integration")]
public class soft_delete_querying : IntegrationContext
{
    public soft_delete_querying(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        // Use a custom schema so the table is created fresh with soft delete columns
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "soft_delete_query";
        });
    }

    [Fact]
    public async Task load_excludes_soft_deleted_documents()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "will-be-deleted" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedDoc>(doc.Id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task query_excludes_soft_deleted_documents()
    {
        var marker = Guid.NewGuid().ToString("N")[..8];
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = $"alive-{marker}" };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = $"dead-{marker}" };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc2);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<SoftDeletedDoc>()
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc1.Id);
    }

    [Fact]
    public async Task maybe_deleted_returns_all_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "alive-maybe" };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "dead-maybe" };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc2);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<SoftDeletedDoc>()
            .MaybeDeleted()
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id)
            .ToListAsync();

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task is_deleted_returns_only_deleted_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "alive-isdeleted" };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "dead-isdeleted" };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc2);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<SoftDeletedDoc>()
            .IsDeleted()
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc2.Id);
    }

    [Fact]
    public async Task deleted_since_filters_by_timestamp()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "recent-delete" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        var beforeDelete = DateTimeOffset.UtcNow.AddSeconds(-1);

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<SoftDeletedDoc>()
            .DeletedSince(beforeDelete)
            .Where(x => x.Id == doc.Id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc.Id);
    }

    [Fact]
    public async Task deleted_before_filters_by_timestamp()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "old-delete" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc);
        await session2.SaveChangesAsync();

        var afterDelete = DateTimeOffset.UtcNow.AddSeconds(1);

        await using var query = theStore.QuerySession();
        var results = await query.Query<SoftDeletedDoc>()
            .DeletedBefore(afterDelete)
            .Where(x => x.Id == doc.Id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc.Id);
    }

    [Fact]
    public async Task load_many_excludes_soft_deleted_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "alive-many" };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "dead-many" };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc2);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.LoadManyAsync<SoftDeletedDoc>(new[] { doc1.Id, doc2.Id });

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe(doc1.Id);
    }

    [Fact]
    public async Task count_excludes_soft_deleted_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "alive-count" };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "dead-count" };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc2);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Query<SoftDeletedDoc>()
            .Where(x => x.Id == doc1.Id || x.Id == doc2.Id)
            .CountAsync();

        count.ShouldBe(1);
    }
}
