using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId.VogenIds;

// Mirrors Marten's ValueTypeTests/VogenIds/int_based_document_operations.
[Collection("integration")]
public class int_based_document_operations : IntegrationContext
{
    public int_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "vogen_int"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var order = new VogenOrder { Name = "Auto" };
        order.Id.ShouldBeNull();

        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        order.Id.ShouldNotBeNull();
        order.Id!.Value.Value.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task store_assigns_hilo_ids()
    {
        var first = new VogenOrder { Name = "First" };
        var second = new VogenOrder { Name = "Second" };

        await using var session = theStore.LightweightSession();
        session.Store(first, second);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        first.Id!.Value.Value.ShouldNotBe(second.Id!.Value.Value);
    }

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new VogenOrder { Name = "Smoke" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<VogenOrder>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9001), Name = "Inserted" };
        await using var session = theStore.LightweightSession();
        session.Insert(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<VogenOrder>(9001, TestContext.Current.CancellationToken);
        loaded!.Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9002), Name = "Original" };
        await using var session = theStore.LightweightSession();
        session.Insert(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        order.Name = "Updated";
        session.Update(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenOrder>(9002, TestContext.Current.CancellationToken))!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9003), Name = "Load Me" };
        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<VogenOrder>(9003, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(order.Id);
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9004), Name = "Identity" };
        await using var session = theStore.IdentitySession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<VogenOrder>(9004, TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<VogenOrder>(9004, TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9005), Name = "Delete" };
        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<VogenOrder>(9005);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenOrder>(9005, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9006), Name = "Delete Doc" };
        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenOrder>(9006, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9007), Name = "LINQ Where" };
        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.Query<VogenOrder>()
            .Where(x => x.Id == order.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        loaded!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new VogenOrder { Id = VogenOrderId.From(9008), Name = "A" });
        session.Store(new VogenOrder { Id = VogenOrderId.From(9009), Name = "B" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenOrder>()
            .OrderBy(x => x.Id)
            .Take(3)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_select_clause()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9010), Name = "Select" };
        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var ids = await session.Query<VogenOrder>()
            .Where(x => x.Id == order.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ids.Single().ShouldBe(order.Id);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        var one = new VogenOrder { Id = VogenOrderId.From(9011), Name = "One" };
        var two = new VogenOrder { Id = VogenOrderId.From(9012), Name = "Two" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenOrder>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task bulk_insert()
    {
        var orders = Enumerable.Range(9100, 5)
            .Select(i => new VogenOrder { Id = VogenOrderId.From(i), Name = $"Bulk {i}" })
            .ToList();

        await theStore.Advanced.BulkInsertAsync(orders, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        foreach (var order in orders)
        {
            (await query.LoadAsync<VogenOrder>(order.Id!.Value.Value, TestContext.Current.CancellationToken))
                .ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(9200), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<VogenOrder>(9200, TestContext.Current.CancellationToken)).ShouldBeTrue();
        (await query.CheckExistsAsync<VogenOrder>(999999, TestContext.Current.CancellationToken)).ShouldBeFalse();
    }
}
