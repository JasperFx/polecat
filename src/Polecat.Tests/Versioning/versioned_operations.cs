using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Versioning;

[Collection("integration")]
public class versioned_operations : IntegrationContext
{
    public versioned_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "versioned_ops";
        });
    }

    [Fact]
    public async Task insert_sets_guid_version()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "first" };
        doc.Version.ShouldBe(Guid.Empty);

        theSession.Insert(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        doc.Version.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task store_new_document_sets_guid_version()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "stored" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        doc.Version.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task store_existing_document_changes_guid_version()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "v1" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        var firstVersion = doc.Version;
        firstVersion.ShouldNotBe(Guid.Empty);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(firstVersion);

        loaded.Name = "v2";
        session2.Store(loaded);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        loaded.Version.ShouldNotBe(firstVersion);
        loaded.Version.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task load_populates_guid_version()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "load-test" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        var savedVersion = doc.Version;

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(savedVersion);
    }

    [Fact]
    public async Task concurrency_check_fails_on_stale_guid_version()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "concurrent" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Load in two sessions
        await using var session1 = theStore.LightweightSession();
        var loaded1 = await session1.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded1.ShouldNotBeNull();

        await using var session2 = theStore.LightweightSession();
        var loaded2 = await session2.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded2.ShouldNotBeNull();

        // First session saves successfully
        loaded1.Name = "updated-by-session1";
        session1.Store(loaded1);
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Second session tries to save with stale version — should fail
        loaded2.Name = "updated-by-session2";
        session2.Store(loaded2);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task update_with_guid_version_check_succeeds()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "update-test" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        var firstVersion = doc.Version;

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(firstVersion);

        loaded.Name = "updated";
        session2.Update(loaded);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded.Version.ShouldNotBe(firstVersion);
    }

    [Fact]
    public async Task update_with_stale_guid_version_throws()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "stale-update" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Load in two sessions
        await using var s1 = theStore.LightweightSession();
        var l1 = await s1.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        l1.ShouldNotBeNull();

        await using var s2 = theStore.LightweightSession();
        var l2 = await s2.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        l2.ShouldNotBeNull();

        // First update succeeds
        l1.Name = "s1-update";
        s1.Update(l1);
        await s1.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Second update fails with stale version
        l2.Name = "s2-update";
        s2.Update(l2);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await s2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task update_expected_version_with_correct_version_succeeds()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "expected-ver" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        var currentVersion = doc.Version;

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<VersionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Name = "updated";
        session2.UpdateExpectedVersion(loaded, currentVersion);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        loaded.Version.ShouldNotBe(currentVersion);
    }

    [Fact]
    public async Task linq_query_populates_guid_version()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "linq-version" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        var savedVersion = doc.Version;

        await using var query = theStore.QuerySession();
        var results = await query.Query<VersionedDoc>()
            .Where(x => x.Id == doc.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        results.Count.ShouldBe(1);
        results[0].Version.ShouldBe(savedVersion);
    }
}
