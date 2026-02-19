using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Versioning;

[Collection("integration")]
public class revisioned_operations : IntegrationContext
{
    public revisioned_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "revisioned_ops";
        });
    }

    [Fact]
    public async Task insert_sets_version_to_1()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "first" };
        doc.Version.ShouldBe(0);

        theSession.Insert(doc);
        await theSession.SaveChangesAsync();

        doc.Version.ShouldBe(1);
    }

    [Fact]
    public async Task store_new_document_sets_version_to_1()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "stored" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        doc.Version.ShouldBe(1);
    }

    [Fact]
    public async Task store_existing_document_increments_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "v1" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();
        doc.Version.ShouldBe(1);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1);

        loaded.Name = "v2";
        session2.Store(loaded);
        await session2.SaveChangesAsync();
        loaded.Version.ShouldBe(2);
    }

    [Fact]
    public async Task load_populates_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "load-test" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<RevisionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1);
    }

    [Fact]
    public async Task load_many_populates_version()
    {
        var doc1 = new RevisionedDoc { Id = Guid.NewGuid(), Name = "many-1" };
        var doc2 = new RevisionedDoc { Id = Guid.NewGuid(), Name = "many-2" };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<RevisionedDoc>(new[] { doc1.Id, doc2.Id });
        loaded.Count.ShouldBe(2);
        loaded.ShouldAllBe(d => d.Version == 1);
    }

    [Fact]
    public async Task concurrency_check_fails_on_stale_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "concurrent" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();
        doc.Version.ShouldBe(1);

        // Simulate a concurrent update: load in two sessions
        await using var session1 = theStore.LightweightSession();
        var loaded1 = await session1.LoadAsync<RevisionedDoc>(doc.Id);
        loaded1.ShouldNotBeNull();

        await using var session2 = theStore.LightweightSession();
        var loaded2 = await session2.LoadAsync<RevisionedDoc>(doc.Id);
        loaded2.ShouldNotBeNull();

        // First session saves successfully
        loaded1.Name = "updated-by-session1";
        session1.Store(loaded1);
        await session1.SaveChangesAsync();
        loaded1.Version.ShouldBe(2);

        // Second session tries to save with stale version (1) — should fail
        loaded2.Name = "updated-by-session2";
        session2.Store(loaded2);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task update_with_revision_check_succeeds()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "update-test" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1);

        loaded.Name = "updated";
        session2.Update(loaded);
        await session2.SaveChangesAsync();
        loaded.Version.ShouldBe(2);
    }

    [Fact]
    public async Task update_with_stale_revision_throws()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "stale-update" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync();

        // Load in two sessions
        await using var s1 = theStore.LightweightSession();
        var l1 = await s1.LoadAsync<RevisionedDoc>(doc.Id);
        l1.ShouldNotBeNull();

        await using var s2 = theStore.LightweightSession();
        var l2 = await s2.LoadAsync<RevisionedDoc>(doc.Id);
        l2.ShouldNotBeNull();

        // First update succeeds
        l1.Name = "s1-update";
        s1.Update(l1);
        await s1.SaveChangesAsync();

        // Second update fails with stale version
        l2.Name = "s2-update";
        s2.Update(l2);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await s2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task update_revision_sets_explicit_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "explicit-rev" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Name = "updated";
        session2.UpdateRevision(loaded, 1); // explicitly set expected revision
        await session2.SaveChangesAsync();
        loaded.Version.ShouldBe(2);
    }

    [Fact]
    public async Task linq_query_populates_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "linq-version" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // Second store to get version 2
        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Name = "updated";
        session2.Store(loaded);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<RevisionedDoc>()
            .Where(x => x.Id == doc.Id)
            .ToListAsync();
        results.Count.ShouldBe(1);
        results[0].Version.ShouldBe(2);
    }
}
