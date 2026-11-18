using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class ReorderedDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
    public string Category { get; set; } = "";
    public int Score { get; set; }
}

public class ReorderedCustomer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int Rank { get; set; }
}

public class ReorderedOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public int Amount { get; set; }
}

// gh-997, mirroring marten#5959. LINQ to Objects sorts stably, so a second OrderBy() re-sorts the whole
// sequence and the earlier ordering is left to break the ties the later one leaves: OrderBy(a).OrderBy(b)
// orders by b, then a. Polecat used to CLEAR the earlier keys, so tied rows came back in whatever order SQL
// Server picked. A key that is already in the ORDER BY is dropped rather than repeated, because SQL Server
// refuses a column named twice there and a repeated key can never break a tie.
//
// Every chain's FIRST ordering ends in a unique key, so LINQ to Objects' order is fully determined.
public class second_order_by_tests : OneOffConfigurationsContext
{
    private static readonly ReorderedDoc[] Docs = Enumerable.Range(1, 8).Select(i => new ReorderedDoc
    {
        Id = Guid.NewGuid(),
        Number = i,
        Name = new[] { "h", "c", "f", "a", "g", "d", "b", "e" }[i - 1],
        Category = i % 2 == 0 ? "x" : "y",
        Score = i % 3
    }).ToArray();

    private async Task SeedAsync()
    {
        // One at a time, out of Number order, so an ordering that goes missing shows up
        foreach (var doc in Docs.OrderBy(x => x.Name))
        {
            await using var session = theStore.LightweightSession();
            session.Store(doc);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
    }

    private static readonly Dictionary<string, Func<IQueryable<ReorderedDoc>, IQueryable<ReorderedDoc>>> Chains = new()
    {
        ["order_by_then_order_by"] = q => q.OrderBy(x => x.Name).OrderBy(x => x.Category),
        ["order_by_then_order_by_descending"] = q => q.OrderBy(x => x.Number).OrderByDescending(x => x.Score),
        ["descending_then_order_by"] = q => q.OrderByDescending(x => x.Name).OrderBy(x => x.Category),
        ["three_order_bys"] = q => q.OrderBy(x => x.Number).OrderBy(x => x.Category).OrderByDescending(x => x.Score),
        ["chain_then_order_by"] = q => q.OrderBy(x => x.Category).ThenBy(x => x.Name).OrderBy(x => x.Score),
        ["order_by_then_chain"] = q =>
            q.OrderByDescending(x => x.Number).OrderBy(x => x.Score).ThenByDescending(x => x.Category),
        ["chain_then_chain"] = q => q.OrderBy(x => x.Score).ThenByDescending(x => x.Number)
            .OrderByDescending(x => x.Category).ThenBy(x => x.Score),
        ["same_key_twice"] = q => q.OrderBy(x => x.Name).OrderByDescending(x => x.Name),
        ["same_key_in_one_chain"] = q => q.OrderBy(x => x.Name).ThenByDescending(x => x.Name),
        ["where_then_reorder"] = q => q.Where(x => x.Number > 2).OrderBy(x => x.Name).OrderBy(x => x.Category),
        ["reorder_then_take"] = q => q.OrderBy(x => x.Number).OrderByDescending(x => x.Category).Take(3),
        ["reorder_then_skip_take"] = q =>
            q.OrderBy(x => x.Name).OrderBy(x => x.Score).ThenByDescending(x => x.Category).Skip(2).Take(4),

        // With gh-996's page: the page's rows arrive in its own order, and LINQ's sort is stable, so the
        // ordering inside the page breaks the ties of the one written after it
        ["order_take_reorder"] = q => q.OrderBy(x => x.Name).Take(6).OrderBy(x => x.Category),
        ["order_skip_take_reorder"] = q =>
            q.OrderByDescending(x => x.Number).Skip(1).Take(6).OrderBy(x => x.Score),
        ["order_take_where_reorder"] = q =>
            q.OrderBy(x => x.Name).Take(6).Where(x => x.Number > 1).OrderByDescending(x => x.Category),
        ["order_take_two_chains"] = q =>
            q.OrderBy(x => x.Name).Take(6).OrderBy(x => x.Score).OrderBy(x => x.Category),
        ["order_take_reorder_by_the_same_key"] = q => q.OrderBy(x => x.Name).Take(5).OrderByDescending(x => x.Name)
    };

    public static TheoryData<string> ChainNames() => new(Chains.Keys);

    [Theory]
    [MemberData(nameof(ChainNames))]
    public async Task chain_matches_linq_to_objects(string name)
    {
        await SeedAsync();
        var chain = Chains[name];
        var expected = chain(Docs.AsQueryable()).Select(x => x.Number).ToList();

        await using var session = theStore.QuerySession();
        var actual = await chain(session.Query<ReorderedDoc>()).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected, name);
    }

    [Fact]
    public async Task a_single_chain_still_orders_correctly()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Category).ThenByDescending(x => x.Score).ThenBy(x => x.Name)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<ReorderedDoc>().OrderBy(x => x.Category)
            .ThenByDescending(x => x.Score).ThenBy(x => x.Name).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    [Fact]
    public async Task the_later_order_by_leads_the_sql_and_the_earlier_breaks_its_ties()
    {
        await using var session = theStore.QuerySession();

        var sql = session.ToSql(session.Query<ReorderedDoc>().OrderBy(x => x.Name).OrderBy(x => x.Category));
        var orderBy = sql[sql.LastIndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase)..];

        orderBy.ShouldBe("ORDER BY JSON_VALUE(data, '$.category'), JSON_VALUE(data, '$.name')", sql);
    }

    [Fact]
    public async Task a_repeated_key_is_named_once()
    {
        await using var session = theStore.QuerySession();

        var sql = session.ToSql(session.Query<ReorderedDoc>().OrderBy(x => x.Score).ThenBy(x => x.Name)
            .OrderByDescending(x => x.Name));
        var orderBy = sql[sql.LastIndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase)..];

        orderBy.ShouldStartWith("ORDER BY JSON_VALUE(data, '$.name') DESC, ", Case.Sensitive, sql);
        orderBy.Split("$.name").Length.ShouldBe(2, sql);
    }

    // The page's ordering stays inside the page AND follows the later ordering outside it
    [Fact]
    public async Task an_ordering_after_a_page_is_tie_broken_by_the_pages_ordering()
    {
        await using var session = theStore.QuerySession();

        var sql = session.ToSql(session.Query<ReorderedDoc>().OrderBy(x => x.Name).Take(6).OrderBy(x => x.Category));
        var outer = sql[sql.LastIndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase)..];

        outer.ShouldBe("ORDER BY JSON_VALUE(data, '$.category'), JSON_VALUE(data, '$.name')", sql);
        sql.ShouldContain("TOP(6)", Case.Sensitive, sql);
    }

    [Fact]
    public async Task last_after_an_ordering_after_a_page()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Name).Take(6).OrderBy(x => x.Category).Last();
        var actual = await session.Query<ReorderedDoc>().OrderBy(x => x.Name).Take(6).OrderBy(x => x.Category)
            .LastAsync(TestContext.Current.CancellationToken);

        actual.Number.ShouldBe(expected.Number);
    }

    [Fact]
    public async Task first_after_a_second_order_by()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number).OrderByDescending(x => x.Score).First();
        var actual = await session.Query<ReorderedDoc>().OrderBy(x => x.Number)
            .OrderByDescending(x => x.Score).FirstAsync(TestContext.Current.CancellationToken);

        actual.Number.ShouldBe(expected.Number);
    }

    [Fact]
    public async Task last_after_a_second_order_by()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Name).OrderBy(x => x.Category).Last();
        var actual = await session.Query<ReorderedDoc>().OrderBy(x => x.Name)
            .OrderBy(x => x.Category).LastAsync(TestContext.Current.CancellationToken);

        actual.Number.ShouldBe(expected.Number);
    }

    private static readonly ReorderedCustomer[] Customers = Enumerable.Range(1, 4)
        .Select(i => new ReorderedCustomer { Id = Guid.NewGuid(), Name = "customer" + i, Rank = i % 2 }).ToArray();

    private static readonly ReorderedOrder[] Orders = Customers.SelectMany((c, i) => Enumerable.Range(0, 3)
        .Select(k => new ReorderedOrder { Id = Guid.NewGuid(), CustomerId = c.Id, Amount = (i + 1) * 10 + k }))
        .ToArray();

    private async Task SeedJoinAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Customers);
        session.Store(Orders);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    // A Select() after the join is not used: the rows are compared as the SelectMany projected them
    private async Task joined<T>(Func<IQueryable<ReorderedCustomer>, IQueryable<ReorderedOrder>, IQueryable<T>> query)
    {
        await SeedJoinAsync();
        var expected = query(Customers.AsQueryable(), Orders.AsQueryable()).ToList();

        await using var session = theStore.QuerySession();
        var actual = await query(session.Query<ReorderedCustomer>(), session.Query<ReorderedOrder>())
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    [Fact]
    public Task a_second_order_by_over_a_join_keeps_the_earlier_as_a_tie_break() => joined((c, o) =>
        c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Rank, y.Amount })
            .OrderByDescending(r => r.Amount).OrderBy(r => r.Rank));

    // gh-997: an outer member over the join used to read $.x.rank, the GroupJoin's anonymous member name
    // kept in the path, so every ordering by an outer member ordered by NULL
    [Fact]
    public Task an_outer_member_orders_the_joined_rows() => joined((c, o) =>
        c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Amount })
            .OrderByDescending(r => r.Name).ThenBy(r => r.Amount));

    [Fact]
    public Task a_repeated_key_over_a_join_is_named_once() => joined((c, o) =>
        c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Rank, y.Amount })
            .OrderBy(r => r.Amount).OrderByDescending(r => r.Rank).ThenByDescending(r => r.Amount));
}
