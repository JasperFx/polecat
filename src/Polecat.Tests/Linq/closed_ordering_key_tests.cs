using System.Linq.Expressions;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1013 (marten#5950, 8e0e856ec): an ordering key that reads nothing from the document -- a literal, a
// static, a captured local, a static array index -- is the same for every row, so LINQ to Objects' stable sort
// ignores it. Polecat drops it rather than refusing it, and never reads a static named like a member off the
// document. Ported from Marten's cases.

public class OrderKeyClosedDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
    public string Category { get; set; } = "";
}

public static class OrderKeyClosedProbe
{
    public static readonly int Value = 3;
    public static readonly string Name = "b";
    public static readonly int[][] Jagged = [[1, 2], [3, 4]];
}

// marten#5950: OrderBy(x => Probe.Value).ThenBy(x => x.Number) threw. In LINQ to Objects a key that reads
// nothing from the element is the same for every element, the stable sort leaves the order to the other keys,
// and a ThenBy() after it acts as the primary ordering. Marten now drops such a key from the ORDER BY.
public class closed_ordering_key_tests : OneOffConfigurationsContext
{
    private static readonly OrderKeyClosedDoc[] Docs = Enumerable.Range(1, 6).Select(i => new OrderKeyClosedDoc
    {
        Id = Guid.NewGuid(),
        Number = i,
        Name = new[] { "f", "c", "a", "e", "b", "d" }[i - 1],
        Category = i % 2 == 0 ? "even" : "odd"
    }).ToArray();

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Docs);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private static Dictionary<string, Func<IQueryable<OrderKeyClosedDoc>, IQueryable<OrderKeyClosedDoc>>> Orderings()
    {
        var local = 7;
        var localName = "z";

        return new()
        {
            ["static_then_member"] = q => q.OrderBy(x => OrderKeyClosedProbe.Value).ThenBy(x => x.Name),
            ["static_then_member_descending"] = q =>
                q.OrderByDescending(x => OrderKeyClosedProbe.Value).ThenByDescending(x => x.Number),
            ["local_then_member"] = q => q.OrderBy(x => local).ThenByDescending(x => x.Name),
            ["literal_then_member"] = q => q.OrderBy(x => 1).ThenBy(x => x.Name),
            ["string_literal_then_member"] = q => q.OrderBy(x => "same").ThenByDescending(x => x.Name),
            ["indexed_static_then_member"] = q =>
                q.OrderBy(x => OrderKeyClosedProbe.Jagged[1][0]).ThenBy(x => x.Name),
            ["arithmetic_then_member"] = q =>
                q.OrderBy(x => OrderKeyClosedProbe.Value * 2 + local).ThenBy(x => x.Name),
            ["member_then_static"] = q =>
                q.OrderBy(x => x.Category).ThenBy(x => OrderKeyClosedProbe.Value).ThenBy(x => x.Number),
            ["member_static_member"] = q =>
                q.OrderBy(x => x.Category).ThenByDescending(x => localName).ThenByDescending(x => x.Name),
            ["static_member_static_member"] = q => q.OrderBy(x => OrderKeyClosedProbe.Name).ThenBy(x => x.Category)
                .ThenByDescending(x => OrderKeyClosedProbe.Value).ThenByDescending(x => x.Number),
            ["where_then_static_then_member"] = q =>
                q.Where(x => x.Number > 2).OrderBy(x => OrderKeyClosedProbe.Value).ThenBy(x => x.Name)
        };
    }

    public static TheoryData<string> OrderingNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in Orderings().Keys) data.Add(name);
        return data;
    }

    // marten#5950
    [Theory]
    [MemberData(nameof(OrderingNames))]
    public async Task closed_key_ordering_matches_linq_to_objects(string name)
    {
        await SeedAsync();
        var order = Orderings()[name];
        var expected = order(Docs.AsQueryable()).Select(x => x.Number).ToList();

        await using var session = theStore.QuerySession();
        var actual = await order(session.Query<OrderKeyClosedDoc>()).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected, name);
    }

    // marten#5950
    [Fact]
    public async Task only_closed_keys_leave_no_order_by()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var local = 5;

        var query = session.Query<OrderKeyClosedDoc>().OrderBy(x => OrderKeyClosedProbe.Value)
            .ThenByDescending(x => local).ThenBy(x => 1);

        session.ToSql(query).ShouldNotContain("ORDER BY", Case.Insensitive);

        var actual = await query.Select(x => x.Number).ToListAsync(TestContext.Current.CancellationToken);
        actual.OrderBy(x => x).ShouldBe(Docs.Select(x => x.Number));
    }

    // marten#5950: before the fix Marten resolved the static's NAME against the document and silently
    // ordered by the document's own Name. This is the parity probe (Marten's answer).
    [Fact]
    public async Task a_static_named_like_a_document_member_is_not_read_from_the_document()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => OrderKeyClosedProbe.Name).ThenByDescending(x => x.Number)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyClosedDoc>().OrderBy(x => OrderKeyClosedProbe.Name)
            .ThenByDescending(x => x.Number).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    // marten#5950: the safety half of the case above -- the static is never read off the document's own Name
    [Fact]
    public async Task a_static_named_like_a_document_member_is_never_read_from_the_document()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => OrderKeyClosedProbe.Name).ThenByDescending(x => x.Number)
            .Select(x => x.Number).ToList();

        var actual = await session.Query<OrderKeyClosedDoc>().OrderBy(x => OrderKeyClosedProbe.Name)
            .ThenByDescending(x => x.Number).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    // marten#5950
    [Fact]
    public async Task first_after_a_closed_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => OrderKeyClosedProbe.Value).ThenByDescending(x => x.Number).First();
        var actual = await session.Query<OrderKeyClosedDoc>().OrderBy(x => OrderKeyClosedProbe.Value)
            .ThenByDescending(x => x.Number).FirstAsync(TestContext.Current.CancellationToken);

        actual.Number.ShouldBe(expected.Number);
    }

    // marten#5950
    [Fact]
    public async Task paging_after_a_closed_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => OrderKeyClosedProbe.Jagged[1][0]).ThenBy(x => x.Name).Skip(1).Take(3)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyClosedDoc>().OrderBy(x => OrderKeyClosedProbe.Jagged[1][0])
            .ThenBy(x => x.Name).Skip(1).Take(3).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    // marten#5950: the comparer of a closed key compares one value with itself, so it is dropped with the key.
    // gh-1011 still refuses a comparer on a key that reads the document.
    [Fact]
    public async Task a_closed_key_with_a_comparer()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => OrderKeyClosedProbe.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(x => x.Name).Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyClosedDoc>()
            .OrderBy(x => OrderKeyClosedProbe.Name, StringComparer.OrdinalIgnoreCase).ThenBy(x => x.Name)
            .Select(x => x.Number).ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }
}
