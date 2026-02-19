using Polecat.Schema.Identity.Sequences;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Documents;

[Collection("integration")]
public class hilo_sequence_tests : IntegrationContext
{
    public hilo_sequence_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_store_and_load_int_id_document()
    {
        var doc = new IntDoc { Name = "Test Int" };
        doc.Id.ShouldBe(0);

        theSession.Store(doc);
        doc.Id.ShouldBeGreaterThan(0);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<IntDoc>(doc.Id);

        loaded.ShouldNotBeNull();
        loaded.Name.ShouldBe("Test Int");
        loaded.Id.ShouldBe(doc.Id);
    }

    [Fact]
    public async Task can_store_and_load_long_id_document()
    {
        var doc = new LongDoc { Name = "Test Long" };
        doc.Id.ShouldBe(0L);

        theSession.Store(doc);
        doc.Id.ShouldBeGreaterThan(0L);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<LongDoc>(doc.Id);

        loaded.ShouldNotBeNull();
        loaded.Name.ShouldBe("Test Long");
        loaded.Id.ShouldBe(doc.Id);
    }

    [Fact]
    public async Task sequential_ids_are_generated()
    {
        var doc1 = new IntDoc { Name = "First" };
        var doc2 = new IntDoc { Name = "Second" };
        var doc3 = new IntDoc { Name = "Third" };

        theSession.Store(doc1);
        theSession.Store(doc2);
        theSession.Store(doc3);

        doc1.Id.ShouldBeGreaterThan(0);
        doc2.Id.ShouldBe(doc1.Id + 1);
        doc3.Id.ShouldBe(doc2.Id + 1);

        await theSession.SaveChangesAsync();
    }

    [Fact]
    public async Task can_establish_hilo_starting_point()
    {
        await theStore.Advanced.ResetHiloSequenceFloor<IntDoc>(2500);

        var doc = new IntDoc { Name = "After floor" };
        theSession.Store(doc);
        doc.Id.ShouldBeGreaterThanOrEqualTo(2500);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<IntDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Name.ShouldBe("After floor");
    }

    [Fact]
    public void default_hilo_settings()
    {
        theStore.Options.HiloSequenceDefaults.MaxLo.ShouldBe(1000);
    }

    [Fact]
    public async Task override_global_hilo_defaults()
    {
        await StoreOptions(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.HiloSequenceDefaults.MaxLo = 55;
        });

        theStore.Options.HiloSequenceDefaults.MaxLo.ShouldBe(55);
    }

    [Fact]
    public async Task attribute_overrides_global()
    {
        await StoreOptions(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.HiloSequenceDefaults.MaxLo = 200;
        });

        var doc = new OverriddenHiloDoc();
        theSession.Store(doc);
        doc.Id.ShouldBeGreaterThan(0);

        await theSession.SaveChangesAsync();

        // The OverriddenHiloDoc has [HiloSequence(MaxLo = 66, SequenceName = "Entity")]
        // so it should use those settings, not the global MaxLo = 200
        var sequence = theStore.Sequences.SequenceFor(typeof(OverriddenHiloDoc));
        sequence.MaxLo.ShouldBe(66);
    }

    [Fact]
    public async Task insert_and_update_with_int_id()
    {
        var doc = new IntDoc { Name = "Original" };

        theSession.Store(doc);
        doc.Id.ShouldBeGreaterThan(0);
        await theSession.SaveChangesAsync();

        var savedId = doc.Id;

        // Update
        doc.Name = "Updated";
        await using var session2 = theStore.LightweightSession();
        session2.Update(doc);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<IntDoc>(savedId);
        loaded.ShouldNotBeNull();
        loaded.Name.ShouldBe("Updated");
        loaded.Id.ShouldBe(savedId);
    }

    [Fact]
    public async Task delete_by_int_id()
    {
        var doc = new IntDoc { Name = "To Delete" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        var savedId = doc.Id;

        await using var session2 = theStore.LightweightSession();
        session2.Delete<IntDoc>(savedId);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<IntDoc>(savedId);
        loaded.ShouldBeNull();
    }
}
