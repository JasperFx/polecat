using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// Dedicated test models for GroupJoin tests to avoid cross-test interference
public class JoinCustomer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
}

public class JoinOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public string Status { get; set; } = "";
    public decimal Amount { get; set; }
}

public class JoinEmployee
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
}

public class group_join_tests : OneOffConfigurationsContext
{
    private Guid _aliceId;
    private Guid _bobId;
    private Guid _charlieId;

    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        _aliceId = Guid.NewGuid();
        _bobId = Guid.NewGuid();
        _charlieId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();

        session.Store(new JoinCustomer { Id = _aliceId, Name = "Alice", City = "Seattle" });
        session.Store(new JoinCustomer { Id = _bobId, Name = "Bob", City = "Portland" });
        session.Store(new JoinCustomer { Id = _charlieId, Name = "Charlie", City = "Seattle" });

        // Alice has 2 orders, Bob has 1, Charlie has 0
        session.Store(new JoinOrder { Id = Guid.NewGuid(), CustomerId = _aliceId, Status = "Shipped", Amount = 100m });
        session.Store(new JoinOrder { Id = Guid.NewGuid(), CustomerId = _aliceId, Status = "Pending", Amount = 250m });
        session.Store(new JoinOrder { Id = Guid.NewGuid(), CustomerId = _bobId, Status = "Delivered", Amount = 50m });

        // Employees in various cities (for cross-type join tests)
        session.Store(new JoinEmployee { Id = Guid.NewGuid(), Name = "Eve", City = "Seattle" });
        session.Store(new JoinEmployee { Id = Guid.NewGuid(), Name = "Frank", City = "Portland" });
        session.Store(new JoinEmployee { Id = Guid.NewGuid(), Name = "Grace", City = "Denver" });

        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task inner_join_basic()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { CustomerName = x.c.Name, o.Amount })
            .ToListAsync(TestContext.Current.CancellationToken);

        // Alice has 2 orders, Bob has 1, Charlie has 0 (excluded by inner join)
        results.Count.ShouldBe(3);
        results.Count(r => r.CustomerName == "Alice").ShouldBe(2);
        results.Count(r => r.CustomerName == "Bob").ShouldBe(1);
        results.ShouldNotContain(r => r.CustomerName == "Charlie");
    }

    [Fact]
    public async Task inner_join_with_projection()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.c.Name, x.c.City, o.Status, o.Amount })
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(3);

        var aliceShipped = results.First(r => r.Name == "Alice" && r.Status == "Shipped");
        aliceShipped.City.ShouldBe("Seattle");
        aliceShipped.Amount.ShouldBe(100m);

        var bobOrder = results.First(r => r.Name == "Bob");
        bobOrder.City.ShouldBe("Portland");
        bobOrder.Status.ShouldBe("Delivered");
    }

    [Fact]
    public async Task left_join_includes_unmatched()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders.DefaultIfEmpty(), (x, o) => new { x.c.Name, Order = o })
            .ToListAsync(TestContext.Current.CancellationToken);

        // Alice: 2 orders, Bob: 1 order, Charlie: 1 null row = 4 total
        results.Count.ShouldBe(4);
        results.Count(r => r.Name == "Alice").ShouldBe(2);
        results.Count(r => r.Name == "Bob").ShouldBe(1);
        results.Count(r => r.Name == "Charlie").ShouldBe(1);

        var charlieRow = results.First(r => r.Name == "Charlie");
        charlieRow.Order.ShouldBeNull();
    }

    [Fact]
    public async Task left_join_with_null_guard()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders.DefaultIfEmpty(),
                (x, o) => new { x.c.Name, Amount = o != null ? o.Amount : 0m })
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(4);

        var charlieRow = results.First(r => r.Name == "Charlie");
        charlieRow.Amount.ShouldBe(0m);

        var aliceTotal = results.Where(r => r.Name == "Alice").Sum(r => r.Amount);
        aliceTotal.ShouldBe(350m);
    }

    [Fact]
    public async Task inner_join_with_where()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        // Where on outer source before the join
        var results = await query.Query<JoinCustomer>()
            .Where(c => c.City == "Seattle")
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.c.Name, o.Amount })
            .ToListAsync(TestContext.Current.CancellationToken);

        // Only Alice is in Seattle with orders (Charlie is in Seattle but has no orders)
        results.Count.ShouldBe(2);
        results.ShouldAllBe(r => r.Name == "Alice");
    }

    [Fact]
    public async Task inner_join_with_order_by()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.c.Name, o.Amount })
            .OrderBy(r => r.Amount)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(3);
        results[0].Amount.ShouldBe(50m);   // Bob's order
        results[1].Amount.ShouldBe(100m);  // Alice's shipped
        results[2].Amount.ShouldBe(250m);  // Alice's pending
    }

    [Fact]
    public async Task inner_join_with_count()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var count = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.c.Name, o.Amount })
            .CountAsync(TestContext.Current.CancellationToken);

        count.ShouldBe(3);
    }

    [Fact]
    public async Task inner_join_with_first()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var result = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.c.Name, o.Amount })
            .OrderByDescending(r => r.Amount)
            .FirstAsync(TestContext.Current.CancellationToken);

        result.Amount.ShouldBe(250m);
        result.Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task inner_join_on_string_property()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        // Join customers to other customers in the same city
        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinCustomer>(),
                c => c.City, c2 => c2.City,
                (c, matches) => new { c, matches })
            .SelectMany(x => x.matches, (x, m) => new { OriginalName = x.c.Name, MatchName = m.Name })
            .ToListAsync(TestContext.Current.CancellationToken);

        // Seattle: Alice-Alice, Alice-Charlie, Charlie-Alice, Charlie-Charlie = 4
        // Portland: Bob-Bob = 1
        // Total = 5
        results.Count.ShouldBe(5);
        results.Count(r => r.OriginalName == "Alice").ShouldBe(2);
        results.Count(r => r.OriginalName == "Charlie").ShouldBe(2);
        results.Count(r => r.OriginalName == "Bob").ShouldBe(1);
    }

    [Fact]
    public async Task inner_join_full_entity_projection()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { Customer = c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.Customer, Order = o })
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(3);

        var aliceOrders = results.Where(r => r.Customer.Name == "Alice").ToList();
        aliceOrders.Count.ShouldBe(2);
        aliceOrders.ShouldAllBe(r => r.Customer.Id == _aliceId);
        aliceOrders.Select(r => r.Order.Amount).OrderBy(a => a).ShouldBe([100m, 250m]);

        var bobRow = results.First(r => r.Customer.Name == "Bob");
        bobRow.Order.CustomerId.ShouldBe(_bobId);
    }

    [Fact]
    public async Task select_outer_only_properties()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        // Result selector only references outer entity properties, no inner fields
        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.c.Name, x.c.City })
            .ToListAsync(TestContext.Current.CancellationToken);

        // One row per matching join pair (Alice:2, Bob:1) = 3
        results.Count.ShouldBe(3);
        results.Count(r => r.City == "Seattle").ShouldBe(2);
        results.Count(r => r.City == "Portland").ShouldBe(1);
    }

    [Fact]
    public async Task group_join_without_select_many_should_throw()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        await Should.ThrowAsync<NotSupportedException>(async () =>
        {
            await query.Query<JoinCustomer>()
                .GroupJoin(query.Query<JoinOrder>(),
                    c => c.Id, o => o.CustomerId,
                    (c, orders) => new { c, orders = orders.ToList() })
                .ToListAsync();
        });
    }

    [Fact]
    public async Task cross_type_inner_join_on_string_field()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        // Join Customer to Employee on City (cross-type string field join)
        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinEmployee>(),
                c => c.City, e => e.City,
                (c, employees) => new { c, employees })
            .SelectMany(x => x.employees,
                (x, e) => new { Customer = x.c.Name, Employee = e.Name, x.c.City })
            .ToListAsync(TestContext.Current.CancellationToken);

        // Seattle: Alice-Eve, Charlie-Eve = 2
        // Portland: Bob-Frank = 1
        // Denver: no customers, so Grace not matched = 0
        results.Count.ShouldBe(3);
        results.Count(r => r.City == "Seattle").ShouldBe(2);
        results.Count(r => r.City == "Portland").ShouldBe(1);
        results.ShouldNotContain(r => r.City == "Denver");
    }

    [Fact]
    public async Task left_join_with_nullable_cast()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        // LEFT JOIN with null-safe amount access (adapted from Marten's (decimal?)o.Amount pattern)
        var results = await query.Query<JoinCustomer>()
            .GroupJoin(query.Query<JoinOrder>(),
                c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders.DefaultIfEmpty(),
                (x, o) => new { CustomerName = x.c.Name, OrderAmount = o != null ? o.Amount : 0m })
            .ToListAsync(TestContext.Current.CancellationToken);

        // Alice:2, Bob:1, Charlie:1 (null inner) = 4
        results.Count.ShouldBe(4);
        results.Count(r => r.CustomerName == "Alice").ShouldBe(2);
        results.Count(r => r.CustomerName == "Bob").ShouldBe(1);
        results.Count(r => r.CustomerName == "Charlie").ShouldBe(1);

        // Charlie's row should have 0 amount (null guard)
        var charlieRow = results.First(r => r.CustomerName == "Charlie");
        charlieRow.OrderAmount.ShouldBe(0m);
    }
}
