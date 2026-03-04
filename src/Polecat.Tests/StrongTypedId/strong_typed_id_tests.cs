using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

// ── Test Document Types with Strongly Typed IDs ──

public record struct OrderId(Guid Value);

public class Order
{
    public OrderId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record struct ItemNumber(int Value);

public class Item
{
    public ItemNumber Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public readonly record struct TaskId
{
    public TaskId(Guid value) => Value = value;
    public Guid Value { get; init; }
    public static TaskId From(Guid value) => new(value);
}

public class TaskDoc
{
    public TaskId Id { get; set; }
    public string Title { get; set; } = string.Empty;
}

[Collection("integration")]
public class strong_typed_id_tests : IntegrationContext
{
    public strong_typed_id_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "strong_typed_id"; });
    }

    [Fact]
    public async Task store_and_load_guid_wrapper()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "Widget" };

        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Order>(orderId.Value);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(orderId);
        loaded.Name.ShouldBe("Widget");
    }

    [Fact]
    public async Task store_assigns_guid_id_when_default()
    {
        var order = new Order { Name = "Auto Assign" };
        order.Id.Value.ShouldBe(Guid.Empty);

        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync();

        order.Id.Value.ShouldNotBe(Guid.Empty);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Order>(order.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Auto Assign");
    }

    [Fact]
    public async Task store_and_load_int_wrapper()
    {
        await using var session = theStore.LightweightSession();
        var item = new Item { Name = "Bolt" };
        session.Store(item);
        await session.SaveChangesAsync();

        item.Id.Value.ShouldBeGreaterThan(0);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Item>(item.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(item.Id);
        loaded.Name.ShouldBe("Bolt");
    }

    [Fact]
    public async Task store_assigns_hilo_id_for_int_wrapper()
    {
        await using var session = theStore.LightweightSession();
        var items = new List<Item>();
        for (var i = 0; i < 5; i++)
        {
            var item = new Item { Name = $"Item {i}" };
            session.Store(item);
            items.Add(item);
        }

        await session.SaveChangesAsync();

        foreach (var item in items)
        {
            item.Id.Value.ShouldBeGreaterThan(0);
        }

        // All IDs should be unique
        items.Select(i => i.Id.Value).Distinct().Count().ShouldBe(5);
    }

    [Fact]
    public async Task store_and_load_builder_pattern()
    {
        var taskId = TaskId.From(Guid.NewGuid());
        var task = new TaskDoc { Id = taskId, Title = "Build Feature" };

        await using var session = theStore.LightweightSession();
        session.Store(task);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<TaskDoc>(taskId.Value);
        loaded.ShouldNotBeNull();
        loaded!.Id.Value.ShouldBe(taskId.Value);
        loaded.Title.ShouldBe("Build Feature");
    }

    [Fact]
    public async Task insert_and_update_with_wrapper_id()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "Original" };

        await using var session1 = theStore.LightweightSession();
        session1.Insert(order);
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        order.Name = "Updated";
        session2.Update(order);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Order>(orderId.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task delete_by_wrapper_id()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "To Delete" };

        await using var session1 = theStore.LightweightSession();
        session1.Store(order);
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete<Order>(orderId.Value);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Order>(orderId.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document_with_wrapper_id()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "Delete By Doc" };

        await using var session1 = theStore.LightweightSession();
        session1.Store(order);
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(order);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Order>(orderId.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task identity_map_with_wrapper_id()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "Identity Map" };

        await using var session = theStore.IdentitySession();
        session.Store(order);
        await session.SaveChangesAsync();

        // Load twice — should return the same reference
        var first = await session.LoadAsync<Order>(orderId.Value);
        var second = await session.LoadAsync<Order>(orderId.Value);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task bulk_insert_with_wrapper_ids()
    {
        var orders = Enumerable.Range(0, 10)
            .Select(i => new Order { Id = new OrderId(Guid.NewGuid()), Name = $"Bulk {i}" })
            .ToList();

        await theStore.Advanced.BulkInsertAsync(orders);

        await using var query = theStore.QuerySession();
        foreach (var order in orders)
        {
            var loaded = await query.LoadAsync<Order>(order.Id.Value);
            loaded.ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task linq_where_by_wrapper_id()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "LINQ Lookup" };

        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<Order>()
            .Where(x => x.Id == orderId)
            .FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result!.Name.ShouldBe("LINQ Lookup");
    }

    [Fact]
    public async Task linq_order_by_wrapper_id()
    {
        var id1 = new OrderId(Guid.NewGuid());
        var id2 = new OrderId(Guid.NewGuid());

        await using var session = theStore.LightweightSession();
        session.Store(new Order { Id = id1, Name = "First" });
        session.Store(new Order { Id = id2, Name = "Second" });
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Order>()
            .OrderBy(x => x.Id)
            .ToListAsync();

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task multiple_document_types_with_different_wrapper_ids()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "Multi Type Order" };

        await using var session = theStore.LightweightSession();
        session.Store(order);
        var item = new Item { Name = "Multi Type Item" };
        session.Store(item);
        await session.SaveChangesAsync();

        item.Id.Value.ShouldBeGreaterThan(0);

        await using var query = theStore.QuerySession();
        var loadedOrder = await query.LoadAsync<Order>(orderId.Value);
        var loadedItem = await query.LoadAsync<Item>(item.Id.Value);

        loadedOrder.ShouldNotBeNull();
        loadedItem.ShouldNotBeNull();
        loadedOrder!.Name.ShouldBe("Multi Type Order");
        loadedItem!.Name.ShouldBe("Multi Type Item");
    }

    [Fact]
    public async Task pending_changes_tracks_wrapper_id_operations()
    {
        var orderId = new OrderId(Guid.NewGuid());
        var order = new Order { Id = orderId, Name = "Pending" };

        await using var session = theStore.LightweightSession();
        session.Store(order);

        session.PendingChanges.HasOutstandingWork().ShouldBeTrue();
        session.PendingChanges.Operations.Count.ShouldBe(1);

        await session.SaveChangesAsync();
        session.PendingChanges.HasOutstandingWork().ShouldBeFalse();
    }
}
