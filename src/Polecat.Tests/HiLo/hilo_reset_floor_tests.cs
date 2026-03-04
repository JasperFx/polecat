using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.HiLo;

[Collection("integration")]
public class hilo_reset_floor_tests : IntegrationContext
{
    public hilo_reset_floor_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "hilo_reset"; });
    }

    [Fact]
    public async Task reset_floor_advances_next_id()
    {
        // Store one doc to initialize the sequence
        await using var session1 = theStore.LightweightSession();
        session1.Store(new IntDoc { Name = "Init" });
        await session1.SaveChangesAsync();

        // Reset floor to 100
        await theStore.Advanced.ResetHiloSequenceFloor<IntDoc>(100);

        // Next doc should get ID > 100
        await using var session2 = theStore.LightweightSession();
        var doc = new IntDoc { Name = "After Reset" };
        session2.Store(doc);
        await session2.SaveChangesAsync();

        doc.Id.ShouldBeGreaterThan(100);
    }

    [Fact]
    public async Task reset_floor_then_store_uses_new_range()
    {
        await theStore.Advanced.ResetHiloSequenceFloor<IntDoc>(200);

        var docs = new List<IntDoc>();
        await using var session = theStore.LightweightSession();
        for (var i = 0; i < 5; i++)
        {
            var doc = new IntDoc { Name = $"Doc {i}" };
            session.Store(doc);
            docs.Add(doc);
        }
        await session.SaveChangesAsync();

        foreach (var doc in docs)
        {
            doc.Id.ShouldBeGreaterThan(200);
        }
    }

    [Fact]
    public async Task reset_floor_zero_is_noop()
    {
        // Store a doc first to get an initial ID
        await using var session1 = theStore.LightweightSession();
        var doc1 = new IntDoc { Name = "Before" };
        session1.Store(doc1);
        await session1.SaveChangesAsync();
        var firstId = doc1.Id;
        firstId.ShouldBeGreaterThan(0);

        // Reset to 0 — should not break subsequent assignments
        await theStore.Advanced.ResetHiloSequenceFloor<IntDoc>(0);

        await using var session2 = theStore.LightweightSession();
        var doc2 = new IntDoc { Name = "After Zero" };
        session2.Store(doc2);
        await session2.SaveChangesAsync();

        doc2.Id.ShouldBeGreaterThan(0);
    }
}
