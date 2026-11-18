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

    public override async ValueTask InitializeAsync()
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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        doc.Version.ShouldBe(1);
    }

    [Fact]
    public async Task store_new_document_sets_version_to_1()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "stored" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        doc.Version.ShouldBe(1);
    }

    /// <summary>
    ///     #559: <c>Store</c> passes the document's own <c>Version</c> as the target revision, and the
    ///     target must be strictly greater than what is stored — so re-storing a document at the
    ///     revision it was loaded at is a concurrency failure, not an increment.
    /// </summary>
    [Fact]
    public async Task store_existing_document_at_the_loaded_revision_is_refused()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "v1" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        doc.Version.ShouldBe(1);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1);

        loaded.Name = "v2";
        session2.Store(loaded);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    /// <summary>
    ///     #559: the two supported ways to move a loaded document forward — name the revision it is
    ///     going to (<c>doc.Version + 1</c>), or hand back <c>Version = 0</c> and let the store
    ///     increment whatever it has.
    /// </summary>
    [Fact]
    public async Task store_existing_document_moves_forward_by_naming_the_next_revision()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "v1" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Name = "v2";
        session2.UpdateRevision(loaded, loaded.Version + 1);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded.Version.ShouldBe(2);

        await using var session3 = theStore.LightweightSession();
        var again = await session3.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        again.ShouldNotBeNull();
        again.Name = "v3";
        again.Version = 0; // the auto escape hatch
        session3.Store(again);
        await session3.SaveChangesAsync(TestContext.Current.CancellationToken);
        again.Version.ShouldBe(3);
    }

    /// <summary>
    ///     #559: an explicit revision may skip ahead, and the row lands at exactly the revision named
    ///     rather than one past it.
    /// </summary>
    [Fact]
    public async Task explicit_revision_may_jump_and_lands_exactly()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "v1" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Name = "imported";
        session2.UpdateRevision(loaded, 10);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded.Version.ShouldBe(10);

        await using var query = theStore.QuerySession();
        var stored = await query.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        stored.ShouldNotBeNull();
        stored.Version.ShouldBe(10);
    }

    /// <summary>
    ///     #559 site 3: an explicit revision on a brand-new document is honoured rather than
    ///     normalised to 1, and the store counts on from it.
    /// </summary>
    [Fact]
    public async Task insert_honours_an_explicit_revision()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "imported", Version = 7 };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        doc.Version.ShouldBe(7);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(7);

        loaded.Name = "moved on";
        loaded.Version = 0;
        session2.Store(loaded);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded.Version.ShouldBe(8);
    }

    [Fact]
    public async Task load_populates_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "load-test" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1);
    }

    [Fact]
    public async Task load_many_populates_version()
    {
        var doc1 = new RevisionedDoc { Id = Guid.NewGuid(), Name = "many-1" };
        var doc2 = new RevisionedDoc { Id = Guid.NewGuid(), Name = "many-2" };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<RevisionedDoc>(new[] { doc1.Id, doc2.Id }, TestContext.Current.CancellationToken);
        loaded.Count.ShouldBe(2);
        loaded.ShouldAllBe(d => d.Version == 1);
    }

    [Fact]
    public async Task concurrency_check_fails_on_stale_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "concurrent" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        doc.Version.ShouldBe(1);

        // Simulate a concurrent update: load in two sessions
        await using var session1 = theStore.LightweightSession();
        var loaded1 = await session1.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded1.ShouldNotBeNull();

        await using var session2 = theStore.LightweightSession();
        var loaded2 = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded2.ShouldNotBeNull();

        // First session saves successfully by naming the revision it is moving to (#559)
        loaded1.Name = "updated-by-session1";
        session1.UpdateRevision(loaded1, loaded1.Version + 1);
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded1.Version.ShouldBe(2);

        // Second session tries to save against the revision it loaded (1) — 1 is not greater than
        // the stored 2, so it is refused
        loaded2.Name = "updated-by-session2";
        session2.UpdateRevision(loaded2, loaded2.Version + 1);

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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1);

        loaded.Name = "updated";
        loaded.Version = 2; // #559: name the target revision, which must exceed the stored 1
        session2.Update(loaded);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded.Version.ShouldBe(2);
    }

    [Fact]
    public async Task update_with_stale_revision_throws()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "stale-update" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Load in two sessions
        await using var s1 = theStore.LightweightSession();
        var l1 = await s1.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        l1.ShouldNotBeNull();

        await using var s2 = theStore.LightweightSession();
        var l2 = await s2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        l2.ShouldNotBeNull();

        // First update succeeds
        l1.Name = "s1-update";
        l1.Version += 1;
        s1.Update(l1);
        await s1.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Second update fails: its target revision (2) no longer exceeds the stored 2
        l2.Name = "s2-update";
        l2.Version += 1;
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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Name = "updated";
        session2.UpdateRevision(loaded, 2); // #559: the TARGET revision, strictly greater than 1
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded.Version.ShouldBe(2);
    }

    [Fact]
    public async Task linq_query_populates_version()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "linq-version" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Second store to get version 2
        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Name = "updated";
        loaded.Version = 0; // #559: auto, rather than re-storing at the loaded revision
        session2.Store(loaded);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var results = await query.Query<RevisionedDoc>()
            .Where(x => x.Id == doc.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        results.Count.ShouldBe(1);
        results[0].Version.ShouldBe(2);
    }
}
