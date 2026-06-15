using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Cleaning;

// polecat#191: a public, per-store reset/clean API on AdvancedOperations parallel to
// Marten — Advanced.ResetAllData() plus an IDocumentCleaner (DeleteAllDocumentsAsync /
// DeleteAllEventDataAsync / CompletelyRemoveAllAsync). The headline requirement is that
// every operation is scoped to the owning store's schema, so an ancillary store
// (AddPolecatStore<T> with its own schema) can reset its own data on boot without ever
// touching the host application's data (CritterWatch.Embedded).

public class ResetDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record ResetHappened(string Name);

public class reset_all_data_tests
{
    // Build a store, ensure its full schema (event tables included) exists, and clear it to a
    // known empty state regardless of rows left behind by prior runs in the fixed schema.
    private static async Task<DocumentStore> NewStoreAsync(string schema)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.Connection(ConnectionSource.ConnectionString);
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await store.Advanced.CleanAllDocumentsAsync();
        await store.Advanced.CleanAllEventDataAsync();
        return store;
    }

    // Seed two documents and one event stream, returning (docCount, eventCount).
    private static async Task SeedAsync(DocumentStore store)
    {
        await using var session = store.LightweightSession();
        session.Store(new ResetDoc { Id = Guid.NewGuid(), Name = "a" });
        session.Store(new ResetDoc { Id = Guid.NewGuid(), Name = "b" });
        session.Events.StartStream(Guid.NewGuid(), new ResetHappened("started"));
        await session.SaveChangesAsync();
    }

    private static async Task<int> DocCountAsync(DocumentStore store)
    {
        await using var query = store.QuerySession();
        return (await query.Query<ResetDoc>().ToListAsync()).Count;
    }

    [Fact]
    public async Task reset_all_data_clears_documents_and_events()
    {
        using var store = await NewStoreAsync("reset_all_basic");

        await SeedAsync(store);
        (await DocCountAsync(store)).ShouldBe(2);
        (await store.Advanced.FetchEventStoreStatistics()).EventCount.ShouldBe(1);

        await store.Advanced.ResetAllData();

        (await DocCountAsync(store)).ShouldBe(0);
        var stats = await store.Advanced.FetchEventStoreStatistics();
        stats.EventCount.ShouldBe(0);
        stats.StreamCount.ShouldBe(0);
    }

    [Fact]
    public async Task document_cleaner_deletes_documents_and_events_independently()
    {
        using var store = await NewStoreAsync("reset_cleaner");
        await SeedAsync(store);

        // DeleteAllDocuments leaves event data alone.
        await store.Advanced.Clean.DeleteAllDocumentsAsync();
        (await DocCountAsync(store)).ShouldBe(0);
        (await store.Advanced.FetchEventStoreStatistics()).EventCount.ShouldBe(1);

        // DeleteAllEventData clears the events.
        await store.Advanced.Clean.DeleteAllEventDataAsync();
        (await store.Advanced.FetchEventStoreStatistics()).EventCount.ShouldBe(0);
    }

    [Fact]
    public async Task reset_is_scoped_to_the_store_schema()
    {
        using var storeA = await NewStoreAsync("reset_iso_a");
        using var storeB = await NewStoreAsync("reset_iso_b");
        foreach (var s in new[] { storeA, storeB })
        {
            await SeedAsync(s);
        }

        // Resetting store A must NOT touch store B's data — the core CritterWatch.Embedded contract.
        await storeA.Advanced.ResetAllData();

        (await DocCountAsync(storeA)).ShouldBe(0);
        (await storeA.Advanced.FetchEventStoreStatistics()).EventCount.ShouldBe(0);

        (await DocCountAsync(storeB)).ShouldBe(2);
        (await storeB.Advanced.FetchEventStoreStatistics()).EventCount.ShouldBe(1);
    }

    [Fact]
    public async Task reset_all_data_reapplies_initial_data()
    {
        var seededId = Guid.NewGuid();
        using var store = DocumentStore.For(opts =>
        {
            opts.Connection(ConnectionSource.ConnectionString);
            opts.DatabaseSchemaName = "reset_initialdata";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.InitialData.Add(async (s, ct) =>
            {
                await using var session = s.LightweightSession();
                session.Store(new ResetDoc { Id = seededId, Name = "seed" });
                await session.SaveChangesAsync(ct);
            });
        });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await store.Advanced.CleanAllDocumentsAsync();
        await store.Advanced.CleanAllEventDataAsync();

        await SeedAsync(store); // 2 extra docs + an event
        (await DocCountAsync(store)).ShouldBe(2);

        await store.Advanced.ResetAllData();

        // Only the InitialData-seeded document should remain after a reset.
        await using var query = store.QuerySession();
        var docs = await query.Query<ResetDoc>().ToListAsync();
        docs.ShouldHaveSingleItem().Id.ShouldBe(seededId);
    }

    [Fact]
    public async Task completely_remove_all_drops_the_tables()
    {
        const string schema = "reset_remove_all";
        using var store = await NewStoreAsync(schema);
        await SeedAsync(store);

        // Tables exist after seeding.
        var before = await SchemaInspector.GetTableNamesAsync(schema);
        before.ShouldContain(t => t.StartsWith("pc_doc_"));
        before.ShouldContain("pc_events");

        await store.Advanced.Clean.CompletelyRemoveAllAsync();

        // All pc_* tables are gone.
        var after = await SchemaInspector.GetTableNamesAsync(schema);
        after.ShouldNotContain(t => t.StartsWith("pc_"));
    }
}
