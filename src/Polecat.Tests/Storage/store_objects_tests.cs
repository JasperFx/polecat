using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     Coverage for IDocumentOperations.StoreObjects(IEnumerable&lt;object&gt;) —
///     the heterogeneous bulk-store entry point added for parity with
///     JasperFx/jasperfx#268's IDocumentOperations surface and to let
///     Wolverine.Polecat drop its per-document reflection workaround
///     (closes JasperFx/polecat#81).
/// </summary>
[Collection("integration")]
public class store_objects_tests : IntegrationContext
{
    public store_objects_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task stores_a_homogeneous_batch_via_runtime_type_dispatch()
    {
        var docs = new object[]
        {
            new GreenDoc { Id = Guid.NewGuid(), Color = "green-1" },
            new GreenDoc { Id = Guid.NewGuid(), Color = "green-2" },
        };

        theSession.StoreObjects(docs);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded1 = await query.LoadAsync<GreenDoc>(((GreenDoc)docs[0]).Id);
        var loaded2 = await query.LoadAsync<GreenDoc>(((GreenDoc)docs[1]).Id);

        loaded1.ShouldNotBeNull();
        loaded1!.Color.ShouldBe("green-1");
        loaded2.ShouldNotBeNull();
        loaded2!.Color.ShouldBe("green-2");
    }

    [Fact]
    public async Task stores_a_heterogeneous_batch_routes_each_to_its_provider()
    {
        var greenId = Guid.NewGuid();
        var redId = Guid.NewGuid();
        var docs = new object[]
        {
            new GreenDoc { Id = greenId, Color = "leaf" },
            new RedDoc { Id = redId, Label = "tomato" },
        };

        theSession.StoreObjects(docs);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<GreenDoc>(greenId))!.Color.ShouldBe("leaf");
        (await query.LoadAsync<RedDoc>(redId))!.Label.ShouldBe("tomato");
    }

    [Fact]
    public async Task skips_null_entries_silently()
    {
        var id = Guid.NewGuid();
        var docs = new object?[]
        {
            null,
            new GreenDoc { Id = id, Color = "alone" },
            null,
        };

        theSession.StoreObjects(docs!);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<GreenDoc>(id))!.Color.ShouldBe("alone");
    }

    [Fact]
    public async Task subsequent_store_objects_updates_existing_documents()
    {
        var id = Guid.NewGuid();

        theSession.StoreObjects(new object[] { new GreenDoc { Id = id, Color = "v1" } });
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.StoreObjects(new object[] { new GreenDoc { Id = id, Color = "v2" } });
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<GreenDoc>(id))!.Color.ShouldBe("v2");
    }

    [Fact]
    public async Task empty_collection_is_a_no_op()
    {
        theSession.StoreObjects(Array.Empty<object>());
        await theSession.SaveChangesAsync(); // should not throw
    }

    public class GreenDoc
    {
        public Guid Id { get; set; }
        public string Color { get; set; } = "";
    }

    public class RedDoc
    {
        public Guid Id { get; set; }
        public string Label { get; set; } = "";
    }
}
