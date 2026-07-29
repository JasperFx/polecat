using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

public record struct OrderItemId(int Value);

public class OrderItem
{
    public OrderItemId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class int_based_document_operations : IntegrationContext
{
    public int_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "strong_int"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var item = new OrderItem { Name = "Auto" };
        item.Id.Value.ShouldBe(0);

        await using var session = theStore.LightweightSession();
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        item.Id.Value.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "Smoke" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.Query<OrderItem>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "Inserted" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "Original" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        item.Name = "Updated";
        await using var session2 = theStore.LightweightSession();
        session2.Update(item);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "Load Me" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(item.Id);
        loaded.Name.ShouldBe("Load Me");
    }

    [Fact]
    public async Task store_assigns_hilo_ids()
    {
        await using var session = theStore.LightweightSession();
        var items = new List<OrderItem>();
        for (var i = 0; i < 5; i++)
        {
            var item = new OrderItem { Name = $"Item {i}" };
            session.Store(item);
            items.Add(item);
        }

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        foreach (var item in items)
        {
            item.Id.Value.ShouldBeGreaterThan(0);
        }

        items.Select(i => i.Id.Value).Distinct().Count().ShouldBe(5);
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        await using var session = theStore.IdentitySession();
        var item = new OrderItem { Name = "Identity" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "Delete" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.Delete<OrderItem>(item.Id.Value);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "Delete Doc" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.Delete(item);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "LINQ Where" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var result = await query.Query<OrderItem>()
            .Where(x => x.Id == item.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        result.ShouldNotBeNull();
        result!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new OrderItem { Name = "A" });
        session.Store(new OrderItem { Name = "B" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var results = await query.Query<OrderItem>()
            .OrderBy(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        await using var session = theStore.LightweightSession();
        var i1 = new OrderItem { Name = "One" };
        var i2 = new OrderItem { Name = "Two" };
        var i3 = new OrderItem { Name = "Three" };
        session.Store(i1);
        session.Store(i2);
        session.Store(i3);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var results = await query.Query<OrderItem>()
            .Where(x => x.Id.IsOneOf(i1.Id, i2.Id, i3.Id))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        await using var session = theStore.LightweightSession();
        var item = new OrderItem { Name = "Exists" };
        session.Store(item);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<OrderItem>(item.Id.Value, TestContext.Current.CancellationToken)).ShouldBeTrue();
        (await query.CheckExistsAsync<OrderItem>(999999, TestContext.Current.CancellationToken)).ShouldBeFalse();
    }
}
