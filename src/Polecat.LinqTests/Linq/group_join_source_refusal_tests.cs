using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1000 (marten#5928, marten#5910, marten#5731): only Where() may come before GroupJoin(...).SelectMany(...),
// the inner source must be a session.Query<T>() (optionally Where-filtered), and the comparer overload is
// refused. Each of these used to run and return wrong rows: Take/Skip before the join paged the JOINED rows,
// an OrderBy before it was dropped, an in-memory inner source was swapped for the database table of its
// element type, and the comparer was silently ignored.
public class group_join_source_refusal_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private Guid _aliceId;
    private Guid _bobId;

    private async Task SeedAsync()
    {
        ConfigureStore(_ => { });

        _aliceId = Guid.NewGuid();
        _bobId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Store(new JoinCustomer { Id = _aliceId, Name = "Alice", City = "Seattle" });
        session.Store(new JoinCustomer { Id = _bobId, Name = "Bob", City = "Portland" });
        session.Store(new JoinCustomer { Id = Guid.NewGuid(), Name = "Charlie", City = "Seattle" });

        session.Store(new JoinOrder { Id = Guid.NewGuid(), CustomerId = _aliceId, Status = "Active", Amount = 100m });
        session.Store(new JoinOrder { Id = Guid.NewGuid(), CustomerId = _aliceId, Status = "Shipped", Amount = 200m });
        session.Store(new JoinOrder { Id = Guid.NewGuid(), CustomerId = _bobId, Status = "Active", Amount = 300m });

        // PORTLAND differs from Portland only by case: an ordinal comparer says no, a CI collation says yes
        session.Store(new JoinEmployee { Id = Guid.NewGuid(), Name = "Eve", City = "Seattle" });
        session.Store(new JoinEmployee { Id = Guid.NewGuid(), Name = "Frank", City = "PORTLAND" });

        await session.SaveChangesAsync(Ct);
    }

    private static async Task ShouldBeRefusedAsync<T>(Func<Task<T>> act, string because)
    {
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(act);
        ex.Message.ShouldContain(because);
    }

    public static IEnumerable<object[]> OperatorsBeforeTheJoin() =>
    [
        ["Take"], ["Skip"], ["OrderBy"], ["OrderByDescending"], ["Select"], ["Distinct"], ["where_skip_take"]
    ];

    [Theory]
    [MemberData(nameof(OperatorsBeforeTheJoin))]
    public async Task an_operator_other_than_where_before_the_join_is_refused(string shape)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var customers = session.Query<JoinCustomer>();
        IQueryable<JoinCustomer> outer = shape switch
        {
            "Take" => customers.OrderBy(c => c.Name).Take(2),
            "Skip" => customers.OrderBy(c => c.Name).Skip(1),
            "OrderBy" => customers.OrderBy(c => c.Name),
            "OrderByDescending" => customers.OrderByDescending(c => c.Name),
            "Select" => customers.Select(c => c),
            "Distinct" => customers.Distinct(),
            "where_skip_take" => customers.Where(c => c.City == "Seattle").OrderBy(c => c.Name).Skip(1).Take(1),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        var operatorName = shape == "where_skip_take" ? "Take" : shape;

        await ShouldBeRefusedAsync(() => outer
                .GroupJoin(session.Query<JoinOrder>(), c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
                .SelectMany(x => x.orders, (x, o) => new { Customer = x.c.Name, o.Amount })
                .ToListAsync(Ct),
            $"only Where() may come before it on the outer source, and this query applies {operatorName}()");
    }

    [Fact]
    public async Task an_in_memory_inner_source_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        // Used to ignore the list and join the pc_doc_joinorder table instead, returning all three orders
        var inMemory = new List<JoinOrder> { new() { Id = Guid.NewGuid(), CustomerId = Guid.NewGuid() } };

        await ShouldBeRefusedAsync(() => session.Query<JoinCustomer>()
                .GroupJoin(inMemory, c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
                .SelectMany(x => x.orders, (x, o) => new { Customer = x.c.Name, o.Amount })
                .ToListAsync(Ct),
            "the inner source must be a Polecat session.Query<T>()");

        await ShouldBeRefusedAsync(() => session.Query<JoinCustomer>()
                .GroupJoin(inMemory.AsQueryable(), c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
                .SelectMany(x => x.orders, (x, o) => new { Customer = x.c.Name, o.Amount })
                .ToListAsync(Ct),
            "the inner source must be a Polecat session.Query<T>()");
    }

    [Fact]
    public async Task an_ordered_inner_source_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        await ShouldBeRefusedAsync(() => session.Query<JoinCustomer>()
                .GroupJoin(session.Query<JoinOrder>().OrderBy(o => o.Amount), c => c.Id, o => o.CustomerId,
                    (c, orders) => new { c, orders })
                .SelectMany(x => x.orders, (x, o) => new { Customer = x.c.Name, o.Amount })
                .ToListAsync(Ct),
            "the inner source may only be session.Query<T>() filtered by Where(), and it applies OrderBy()");
    }

    [Fact]
    public async Task the_comparer_overload_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        // Used to drop the comparer and match Portland to PORTLAND under the server's CI collation
        await ShouldBeRefusedAsync(() => session.Query<JoinCustomer>()
                .GroupJoin(session.Query<JoinEmployee>(), c => c.City, e => e.City, (c, es) => new { c, es },
                    StringComparer.Ordinal)
                .SelectMany(x => x.es, (x, e) => new { Customer = x.c.Name, Employee = e.Name })
                .ToListAsync(Ct),
            "IEqualityComparer");
    }

    [Fact]
    public async Task a_join_of_a_join_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        await ShouldBeRefusedAsync(() => session.Query<JoinCustomer>()
                .GroupJoin(session.Query<JoinOrder>(), c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
                .SelectMany(x => x.orders, (x, o) => new { x.c.City, o.Amount })
                .GroupJoin(session.Query<JoinEmployee>(), x => x.City, e => e.City, (x, employees) => new { x, employees })
                .SelectMany(y => y.employees, (y, e) => new { y.x.Amount, e.Name })
                .ToListAsync(Ct),
            "Polecat translates one join per query");
    }

    // The control: Where() before the join, on either source, still translates and still filters.
    [Fact]
    public async Task where_before_the_join_on_either_source_still_filters()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var rows = await session.Query<JoinCustomer>()
            .Where(c => c.City == "Seattle")
            .GroupJoin(session.Query<JoinOrder>().Where(o => o.Amount > 150m), c => c.Id, o => o.CustomerId,
                (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { Customer = x.c.Name, o.Amount })
            .ToListAsync(Ct);

        rows.Select(x => (x.Customer, x.Amount)).ShouldBe([("Alice", 200m)]);
    }
}
