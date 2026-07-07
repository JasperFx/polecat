using Polecat.Tests.Harness;
using Weasel.Storage;

namespace Polecat.Tests.Storage;

/// <summary>
///     #273 phase E1: end-to-end round-trips through the closed-shape storage layer resolved via
///     the shared seams (IStorageSession.StorageFor / IProviderGraph) — the shared runtime doing
///     real document work against SQL Server while the bespoke pipeline still drives sessions.
/// </summary>
public class closed_shape_storage_tests : OneOffConfigurationsContext
{
    private async Task executeAsync(IStorageSession session, Weasel.Storage.IStorageOperation operation)
    {
        var builder = new Weasel.SqlServer.BatchBuilder { TenantId = session.TenantId };
        operation.ConfigureCommand(builder, session);
        var batch = builder.Compile();

        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        batch.Connection = conn;

        var exceptions = new List<Exception>();
        await using (var reader = await batch.ExecuteReaderAsync())
        {
            await operation.PostprocessAsync(reader, exceptions, CancellationToken.None);
        }

        foreach (var ex in exceptions) throw ex;
    }

    private async Task<T> bootstrapAsync<T>(T doc) where T : notnull
    {
        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync();
        return doc;
    }

    [Fact]
    public async Task upsert_and_load_round_trip_through_storage_for()
    {
        await bootstrapAsync(new Target { Number = 1 }); // table creation

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;
        var storage = (IDocumentStorage<Target, Guid>)session.StorageFor<Target>();

        var doc = new Target { Number = 42, Color = "green" };
        storage.Store(session, doc);
        doc.Id.ShouldNotBe(Guid.Empty); // sequential Guid assigned by the shared identity strategy

        await executeAsync(session, storage.Upsert(doc, session, session.TenantId));

        var loaded = await storage.LoadAsync(doc.Id, session, CancellationToken.None);
        loaded.ShouldNotBeNull();
        loaded.Number.ShouldBe(42);

        // Bespoke pipeline agrees
        await using var check = theStore.QuerySession();
        (await check.LoadAsync<Target>(doc.Id)).ShouldNotBeNull();
    }

    [Fact]
    public async Task load_many_round_trips_openjson_ids()
    {
        await bootstrapAsync(new Target { Number = 1 });

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;
        var storage = (IDocumentStorage<Target, Guid>)session.StorageFor<Target>();

        var docs = Enumerable.Range(0, 5).Select(i => new Target { Number = 100 + i }).ToArray();
        foreach (var doc in docs)
        {
            storage.Store(session, doc);
            await executeAsync(session, storage.Upsert(doc, session, session.TenantId));
        }

        var ids = docs.Select(d => d.Id).ToArray();
        var loaded = await storage.LoadManyAsync(ids, session, CancellationToken.None);
        loaded.Count.ShouldBe(5);
        loaded.Select(d => d.Number).OrderBy(n => n).ShouldBe([100, 101, 102, 103, 104]);
    }

    [Fact]
    public async Task identity_map_flavor_returns_same_reference_and_serves_from_map()
    {
        var seed = await bootstrapAsync(new Target { Number = 7, Color = "map" });

        await using var raw = theStore.IdentitySession();
        var session = (IStorageSession)raw;
        var storage = (IDocumentStorage<Target, Guid>)session.StorageFor<Target>();

        var first = await storage.LoadAsync(seed.Id, session, CancellationToken.None);
        var second = await storage.LoadAsync(seed.Id, session, CancellationToken.None);

        first.ShouldNotBeNull();
        second.ShouldBeSameAs(first); // second hit came from the shared ItemMap
        session.ItemMap.ContainsKey(typeof(Target)).ShouldBeTrue();
    }

    [Fact]
    public async Task optimistic_storage_round_trips_versions_through_session_tracker()
    {
        await bootstrapAsync(new VersionedDoc { Name = "seed" });

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;
        var storage = (IDocumentStorage<VersionedDoc, Guid>)session.StorageFor<VersionedDoc>();

        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "v1" };
        await executeAsync(session, storage.Upsert(doc, session, session.TenantId));

        // The shared operation stored the new version in the session tracker
        var tracked = storage.VersionFor(doc, session);
        tracked.ShouldNotBeNull();

        // Second upsert with the tracked version succeeds
        doc.Name = "v2";
        await executeAsync(session, storage.Upsert(doc, session, session.TenantId));

        // A different session with a stale expectation gets a concurrency violation
        await using var rawStale = theStore.LightweightSession();
        var staleSession = (IStorageSession)rawStale;
        var staleStorage = (IDocumentStorage<VersionedDoc, Guid>)staleSession.StorageFor<VersionedDoc>();
        staleSession.Versions.StoreVersion<VersionedDoc, Guid>(doc.Id, Guid.NewGuid());
        await Should.ThrowAsync<JasperFx.ConcurrencyException>(
            () => executeAsync(staleSession, staleStorage.Upsert(doc, staleSession, staleSession.TenantId)));
    }

    [Fact]
    public async Task numeric_storage_auto_increments_and_tracks_revisions()
    {
        await bootstrapAsync(new RevisionedDoc { Name = "seed" });

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;
        var storage = (IDocumentStorage<RevisionedDoc, Guid>)session.StorageFor<RevisionedDoc>();

        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "r1" };
        await executeAsync(session, storage.Upsert(doc, session, session.TenantId));
        doc.Version.ShouldBe(1);
        session.Versions.RevisionFor<RevisionedDoc, Guid>(doc.Id).ShouldBe(1);

        doc.Name = "r2";
        await executeAsync(session, storage.Upsert(doc, session, session.TenantId));
        doc.Version.ShouldBe(2);
    }

    [Fact]
    public async Task deletions_hard_and_soft_work_through_the_shared_contract()
    {
        // Hard delete
        var target = await bootstrapAsync(new Target { Number = 5 });
        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;
        var storage = (IDocumentStorage<Target, Guid>)session.StorageFor<Target>();

        await executeAsync(session, storage.DeleteForId(target.Id, session.TenantId));
        (await storage.LoadAsync(target.Id, session, CancellationToken.None)).ShouldBeNull();

        // Soft delete: row survives, loads filter it out
        var soft = await bootstrapAsync(new SoftDeletedDoc { Name = "bye" });
        var softStorage = (IDocumentStorage<SoftDeletedDoc, Guid>)session.StorageFor<SoftDeletedDoc>();
        await executeAsync(session, softStorage.DeleteForId(soft.Id, session.TenantId));

        (await softStorage.LoadAsync(soft.Id, session, CancellationToken.None)).ShouldBeNull();
        await using var check = theStore.QuerySession();
        (await check.LoadAsync<SoftDeletedDoc>(soft.Id)).ShouldBeNull(); // bespoke agrees

        // Hard delete punches through the soft-delete style
        await executeAsync(session, softStorage.HardDeleteForId(soft.Id, session.TenantId));
    }

    [Fact]
    public async Task int_and_string_id_documents_resolve_and_round_trip()
    {
        await bootstrapAsync(new IntDoc { Name = "seed" });
        await bootstrapAsync(new StringDoc { Id = "seed", Name = "seed" });

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        // Hi-Lo int id assigned through the shared identity strategy + ISequenceSource
        var intStorage = (IDocumentStorage<IntDoc, int>)session.StorageFor<IntDoc>();
        var intDoc = new IntDoc { Name = "closed-shape" };
        intStorage.Store(session, intDoc);
        intDoc.Id.ShouldBeGreaterThan(0);
        await executeAsync(session, intStorage.Upsert(intDoc, session, session.TenantId));
        (await intStorage.LoadAsync(intDoc.Id, session, CancellationToken.None)).ShouldNotBeNull();

        var stringStorage = (IDocumentStorage<StringDoc, string>)session.StorageFor<StringDoc>();
        var stringDoc = new StringDoc { Id = "cs-1", Name = "closed-shape" };
        await executeAsync(session, stringStorage.Upsert(stringDoc, session, session.TenantId));
        (await stringStorage.LoadAsync("cs-1", session, CancellationToken.None)).ShouldNotBeNull();
    }

    [Fact]
    public async Task hierarchy_round_trips_subclasses_through_the_hierarchical_selector()
    {
        ConfigureStore(opts => opts.Schema.For<Target>().AddSubClass<HierTarget>());
        await bootstrapAsync(new Target { Number = 1 });

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;
        var storage = (IDocumentStorage<Target, Guid>)session.StorageFor<Target>();

        var child = new HierTarget { Number = 9, Extra = "sub" };
        storage.Store(session, child);
        await executeAsync(session, storage.Upsert(child, session, session.TenantId));

        var loaded = await storage.LoadAsync(child.Id, session, CancellationToken.None);
        loaded.ShouldBeOfType<HierTarget>().Extra.ShouldBe("sub");
    }

    public class HierTarget : Target
    {
        public string Extra { get; set; } = string.Empty;
    }

    [Fact]
    public async Task set_identity_from_string_handles_plain_and_strongly_typed_ids()
    {
        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        // Plain Guid id
        var guidStorage = (IDocumentStorage<Target, Guid>)session.StorageFor<Target>();
        var target = new Target();
        var guid = Guid.NewGuid();
        guidStorage.SetIdentityFromString(target, guid.ToString());
        target.Id.ShouldBe(guid);

        // Strongly-typed wrapper id (#273 E2e): the string converts to the inner value
        // shape, then wraps via the mapping's compiled wrapper.
        var typedStorage = (IDocumentStorage<StrongTypedId.Invoice, StrongTypedId.InvoiceId>)
            session.StorageFor<StrongTypedId.Invoice>();
        var invoice = new StrongTypedId.Invoice();
        var inner = Guid.NewGuid();
        typedStorage.SetIdentityFromString(invoice, inner.ToString());
        invoice.Id.Value.ShouldBe(inner);

        var viaGuid = new StrongTypedId.Invoice();
        var inner2 = Guid.NewGuid();
        typedStorage.SetIdentityFromGuid(viaGuid, inner2);
        viaGuid.Id.Value.ShouldBe(inner2);
    }
}
