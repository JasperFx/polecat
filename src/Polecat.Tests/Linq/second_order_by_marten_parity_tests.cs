using System.Linq.Expressions;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-997 / gh-1013 (marten#5959, d681760e3): a second OrderBy() re-sorts and the earlier keys break its ties,
// closed keys included. Ported from Marten's cases.

public class OrderKeyReorderDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
    public string Category { get; set; } = "";
    public int Score { get; set; }
}

public static class OrderKeyReorderProbe
{
    public static readonly int Value = 3;
}

// marten#5959: a second OrderBy() re-sorts the whole sequence in LINQ to Objects, and the sort is stable, so
// the earlier ordering only breaks the ties the later one leaves: OrderBy(a).OrderBy(b) orders by b, then a.
// Marten decided to mirror exactly that (the later chain leads, earlier chains follow as tie-breakers).
// Polecat's HandleOrderBy CLEARS the earlier keys on a second OrderBy(), so the tie-breaks are lost.
// Each chain's FIRST ordering ends in a unique key, so LINQ to Objects' order is fully determined.
public class second_order_by_marten_parity_tests : OneOffConfigurationsContext
{
    private static readonly OrderKeyReorderDoc[] Docs = Enumerable.Range(1, 8).Select(i => new OrderKeyReorderDoc
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

    private static Dictionary<string, Func<IQueryable<OrderKeyReorderDoc>, IQueryable<OrderKeyReorderDoc>>> Chains()
    {
        var local = 5;

        return new()
        {
            ["order_by_then_order_by"] = q => q.OrderBy(x => x.Name).OrderBy(x => x.Category),
            ["order_by_then_order_by_descending"] = q => q.OrderBy(x => x.Number).OrderByDescending(x => x.Score),
            ["descending_then_order_by"] = q => q.OrderByDescending(x => x.Name).OrderBy(x => x.Category),
            ["three_order_bys"] = q =>
                q.OrderBy(x => x.Number).OrderBy(x => x.Category).OrderByDescending(x => x.Score),
            ["chain_then_order_by"] = q => q.OrderBy(x => x.Category).ThenBy(x => x.Name).OrderBy(x => x.Score),
            ["order_by_then_chain"] = q =>
                q.OrderByDescending(x => x.Number).OrderBy(x => x.Score).ThenByDescending(x => x.Category),
            ["chain_then_chain"] = q => q.OrderBy(x => x.Score).ThenByDescending(x => x.Number)
                .OrderByDescending(x => x.Category).ThenBy(x => x.Score),
            ["same_key_twice"] = q => q.OrderBy(x => x.Name).OrderByDescending(x => x.Name),
            // These three also need marten#5950 (a closed key); they fail on Polecat for that reason too
            ["closed_order_by_after_a_key"] = q => q.OrderBy(x => x.Name).OrderBy(x => OrderKeyReorderProbe.Value),
            ["closed_order_by_between_keys"] = q =>
                q.OrderBy(x => x.Number).OrderBy(x => local).ThenByDescending(x => x.Category),
            ["key_after_closed_order_by"] = q => q.OrderBy(x => OrderKeyReorderProbe.Value).ThenBy(x => x.Name)
                .OrderBy(x => x.Score),
            ["where_then_reorder"] = q => q.Where(x => x.Number > 2).OrderBy(x => x.Name).OrderBy(x => x.Category),
            ["reorder_then_take"] = q => q.OrderBy(x => x.Number).OrderByDescending(x => x.Category).Take(3),
            ["reorder_then_skip_take"] = q =>
                q.OrderBy(x => x.Name).OrderBy(x => x.Score).ThenByDescending(x => x.Category).Skip(2).Take(4)
        };
    }

    public static TheoryData<string> ChainNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in Chains().Keys) data.Add(name);
        return data;
    }

    // marten#5959
    [Theory]
    [MemberData(nameof(ChainNames))]
    public async Task chain_matches_linq_to_objects(string name)
    {
        await SeedAsync();
        var chain = Chains()[name];
        var expected = chain(Docs.AsQueryable()).Select(x => x.Number).ToList();

        await using var session = theStore.QuerySession();
        var actual = await chain(session.Query<OrderKeyReorderDoc>()).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected, name);
    }

    // marten#5959: a single OrderBy().ThenBy() chain is the shape every existing query has
    [Fact]
    public async Task a_single_chain_still_orders_correctly()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Category).ThenByDescending(x => x.Score).ThenBy(x => x.Name)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyReorderDoc>().OrderBy(x => x.Category)
            .ThenByDescending(x => x.Score).ThenBy(x => x.Name).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    // marten#5959: the later OrderBy() leads the SQL and the earlier one follows it as a tie-break
    [Fact]
    public async Task the_earlier_order_by_survives_as_a_tie_break_in_the_sql()
    {
        await using var session = theStore.QuerySession();

        var sql = session.ToSql(session.Query<OrderKeyReorderDoc>().OrderBy(x => x.Name).OrderBy(x => x.Category));
        var orderBy = sql[sql.LastIndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase)..];

        orderBy.ShouldContain("ategory", Case.Sensitive, sql);
        orderBy.ShouldContain("ame", Case.Sensitive, sql);
        orderBy.IndexOf("ategory", StringComparison.Ordinal)
            .ShouldBeLessThan(orderBy.IndexOf("ame", StringComparison.Ordinal), sql);
    }

    // marten#5959
    [Fact]
    public async Task first_after_a_second_order_by()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number).OrderByDescending(x => x.Score).First();
        var actual = await session.Query<OrderKeyReorderDoc>().OrderBy(x => x.Number)
            .OrderByDescending(x => x.Score).FirstAsync(TestContext.Current.CancellationToken);

        actual.Number.ShouldBe(expected.Number);
    }
}
