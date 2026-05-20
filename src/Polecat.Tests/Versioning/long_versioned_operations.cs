using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Versioning;

[Collection("integration")]
public class long_versioned_operations : IntegrationContext
{
    private const string Schema = "long_versioned_ops";
    private const string QualifiedTable = "[long_versioned_ops].[pc_doc_longversioneddoc]";

    public long_versioned_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
        });
    }

    [Fact]
    public async Task insert_sets_version_to_1()
    {
        var doc = new LongVersionedDoc { Id = Guid.NewGuid(), Name = "first" };
        doc.Version.ShouldBe(0L);

        theSession.Insert(doc);
        await theSession.SaveChangesAsync();

        doc.Version.ShouldBe(1L);
    }

    [Fact]
    public async Task store_existing_document_increments_version()
    {
        var doc = new LongVersionedDoc { Id = Guid.NewGuid(), Name = "v1" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();
        doc.Version.ShouldBe(1L);

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<LongVersionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1L);

        loaded.Name = "v2";
        session2.Store(loaded);
        await session2.SaveChangesAsync();
        loaded.Version.ShouldBe(2L);
    }

    [Fact]
    public async Task load_and_linq_populate_version()
    {
        var doc = new LongVersionedDoc { Id = Guid.NewGuid(), Name = "load-test" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<LongVersionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(1L);

        var results = await query.Query<LongVersionedDoc>()
            .Where(x => x.Id == doc.Id)
            .ToListAsync();
        results.Count.ShouldBe(1);
        results[0].Version.ShouldBe(1L);
    }

    [Fact]
    public async Task concurrency_check_fails_on_stale_version()
    {
        var doc = new LongVersionedDoc { Id = Guid.NewGuid(), Name = "concurrent" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session1 = theStore.LightweightSession();
        var loaded1 = await session1.LoadAsync<LongVersionedDoc>(doc.Id);
        loaded1.ShouldNotBeNull();

        await using var session2 = theStore.LightweightSession();
        var loaded2 = await session2.LoadAsync<LongVersionedDoc>(doc.Id);
        loaded2.ShouldNotBeNull();

        loaded1.Name = "updated-by-session1";
        session1.Store(loaded1);
        await session1.SaveChangesAsync();
        loaded1.Version.ShouldBe(2L);

        loaded2.Name = "updated-by-session2";
        session2.Store(loaded2);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task update_revision_long_overload_sets_explicit_version()
    {
        var doc = new LongVersionedDoc { Id = Guid.NewGuid(), Name = "explicit-rev" };

        theSession.Insert(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<LongVersionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Name = "updated";
        session2.UpdateRevision(loaded, 1L); // explicitly set expected revision via the long overload
        await session2.SaveChangesAsync();
        loaded.Version.ShouldBe(2L);
    }

    [Fact]
    public async Task version_beyond_int32_max_round_trips_and_enforces_concurrency()
    {
        var doc = new LongVersionedDoc { Id = Guid.NewGuid(), Name = "big" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();
        doc.Version.ShouldBe(1L);

        // A value comfortably past Int32.MaxValue (2,147,483,647) — the kind of global event
        // sequence number a long-running MultiStreamProjection view would carry.
        const long bigVersion = (long)int.MaxValue + 1000;

        // Seed the revision directly in the DB, beyond anything int could hold.
        await using (var conn = await OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"UPDATE {QualifiedTable} SET version = @v WHERE id = @id";
            cmd.Parameters.AddWithValue("@v", bigVersion);
            cmd.Parameters.AddWithValue("@id", doc.Id);
            (await cmd.ExecuteNonQueryAsync()).ShouldBe(1);
        }

        // The normal load path must surface the full 64-bit value (a GetInt32 read would throw here).
        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<LongVersionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Version.ShouldBe(bigVersion);

        // Storing with the big expected revision succeeds and increments past the Int32 range.
        loaded.Name = "big-updated";
        session2.Store(loaded);
        await session2.SaveChangesAsync();
        loaded.Version.ShouldBe(bigVersion + 1);

        // A stale big revision must still trip the concurrency check.
        await using var s1 = theStore.LightweightSession();
        var l1 = await s1.LoadAsync<LongVersionedDoc>(doc.Id);
        l1.ShouldNotBeNull();
        await using var s2 = theStore.LightweightSession();
        var l2 = await s2.LoadAsync<LongVersionedDoc>(doc.Id);
        l2.ShouldNotBeNull();

        l1.Name = "s1";
        s1.Store(l1);
        await s1.SaveChangesAsync();
        l1.Version.ShouldBe(bigVersion + 2);

        l2.Name = "s2";
        s2.Store(l2);
        await Should.ThrowAsync<ConcurrencyException>(async () => await s2.SaveChangesAsync());
    }
}
