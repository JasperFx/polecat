using System.Linq.Expressions;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1017 (marten#5955, cccd16137): OrderBy() after a Select() orders the projected values, computed ones
// included, by reading the ordering through the projection. Ported from Marten's cases.

public enum OrderKeyColour
{
    // Declared out of alphabetical order, so a name-ordered enum shows up
    Red,
    Green,
    Blue
}

public class OrderKeyProjDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
    public OrderKeyColour Colour { get; set; }
    public OrderKeyProjDoc? Inner { get; set; }
}

public class OrderKeyProjChildParent
{
    public Guid Id { get; set; }
    public int[] Values { get; set; } = [];
}

// marten#5955: OrderBy() after a top-level Select() was dropped, or applied to the documents rather than to
// the projected values. Every shape is compared, in order, with LINQ to Objects, including paging, Count(),
// Any() and First()/FirstOrDefault() after the ordering.
public class order_by_after_projection_tests : OneOffConfigurationsContext
{
    private static readonly string[] Pool = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"];

    private static readonly int[] StorageOrder = [3, 8, 1, 6, 4, 7, 2, 5, 10, 9];

    private static readonly OrderKeyProjDoc[] Docs = StorageOrder.Select(i => new OrderKeyProjDoc
    {
        Id = Guid.NewGuid(),
        Number = i,
        Name = Pool[i % Pool.Length] + i,
        Colour = (OrderKeyColour)(i % 3),
        Inner = new OrderKeyProjDoc { Id = Guid.NewGuid(), Number = 100 - i, Name = "inner" + i }
    }).ToArray();

    private async Task SeedAsync()
    {
        // One at a time, out of Number order, so an ordering that goes missing shows up
        foreach (var doc in Docs)
        {
            await using var session = theStore.LightweightSession();
            session.Store(doc);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    ///     The rows in order, then a page of them, Count(), Any() and First()/FirstOrDefault().
    /// </summary>
    private async Task matches<T>(string shape, Func<IQueryable<OrderKeyProjDoc>, IQueryable<T>> query)
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;
        var expected = query(Docs.AsQueryable()).ToList();

        await using var session = theStore.QuerySession();

        (await query(session.Query<OrderKeyProjDoc>()).ToListAsync(token)).ShouldBe(expected, shape);
        (await query(session.Query<OrderKeyProjDoc>()).Skip(2).Take(3).ToListAsync(token))
            .ShouldBe(expected.Skip(2).Take(3), shape + " Skip(2).Take(3)");
        (await query(session.Query<OrderKeyProjDoc>()).Take(1).ToListAsync(token))
            .ShouldBe(expected.Take(1), shape + " Take(1)");
        (await query(session.Query<OrderKeyProjDoc>()).CountAsync(token)).ShouldBe(expected.Count, shape + " Count");
        (await query(session.Query<OrderKeyProjDoc>()).AnyAsync(token)).ShouldBe(expected.Any(), shape + " Any");
        (await query(session.Query<OrderKeyProjDoc>()).FirstOrDefaultAsync(token))
            .ShouldBe(expected.FirstOrDefault(), shape + " FirstOrDefault");

        if (expected.Any())
        {
            (await query(session.Query<OrderKeyProjDoc>()).FirstAsync(token)).ShouldBe(expected.First(), shape + " First");
        }
    }

    public static TheoryData<string> ProjectedShapes =>
    [
        "member", "member_descending", "member_negation", "member_modulo_then_member", "string_member",
        "string_member_length_then_member", "enum_member", "computed", "computed_descending", "where_before",
        "distinct", "paged_before", "paged_before_member", "same_type_member_scalar"
    ];

    // marten#5955. "where_before" replaces Marten's "where_after" (a Where over the projected scalar, which is
    // a different, filtering gap in Polecat); "paged_before_member" is "paged_before" with a plain member
    // projection, so the Take()-then-OrderBy() half is probed without a computed Select().
    [Theory]
    [MemberData(nameof(ProjectedShapes))]
    public Task after_select(string shape) => shape switch
    {
        "member" => matches(shape, q => q.Select(x => x.Number).OrderBy(n => n)),
        "member_descending" => matches(shape, q => q.Select(x => x.Number).OrderByDescending(n => n)),
        "member_negation" => matches(shape, q => q.Select(x => x.Number).OrderBy(n => -n)),
        "member_modulo_then_member" => matches(shape,
            q => q.Select(x => x.Number).OrderBy(n => n % 3).ThenBy(n => n)),
        "string_member" => matches(shape, q => q.Select(x => x.Name).OrderByDescending(n => n)),
        "string_member_length_then_member" => matches(shape,
            q => q.Select(x => x.Name).OrderBy(n => n.Length).ThenBy(n => n)),
        "enum_member" => matches(shape, q => q.Select(x => x.Colour).OrderBy(c => c)),
        "computed" => matches(shape, q => q.Select(x => x.Number * 7 % 11).OrderBy(n => n)),
        "computed_descending" => matches(shape, q => q.Select(x => x.Name + "z" + x.Number).OrderByDescending(n => n)),
        "where_before" => matches(shape,
            q => q.Where(x => x.Number > 3).Select(x => x.Number).OrderByDescending(n => n)),
        "distinct" => matches(shape, q => q.Select(x => x.Number % 4).Distinct().OrderBy(n => n)),
        "paged_before" => matches(shape,
            q => q.OrderBy(x => x.Number).Take(6).Select(x => x.Number * 3).OrderByDescending(n => n)),
        "paged_before_member" => matches(shape,
            q => q.OrderBy(x => x.Number).Take(6).Select(x => x.Number).OrderByDescending(n => n)),
        "same_type_member_scalar" => matches(shape, q => q.Select(x => x.Inner!.Number).OrderBy(n => n)),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    // marten#5955 "paged_before", without a projection: an ordering written after Take() re-sorts the page
    // Take() chose; it does not choose a different page.
    [Fact]
    public async Task an_ordering_after_take_reorders_the_page_rather_than_choosing_another()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number).Take(4).OrderByDescending(x => x.Name)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyProjDoc>().OrderBy(x => x.Number).Take(4)
            .OrderByDescending(x => x.Name).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    public static TheoryData<string> MartenRefusedShapes => ["anonymous_type", "same_type_document"];

    // marten#5955 refuses these two because it could not order them correctly ("same_type_document" used to
    // order the DOCUMENTS by their own Number). Polecat reads a key through the Select (#875), so it orders them.
    [Theory]
    [MemberData(nameof(MartenRefusedShapes))]
    public async Task marten_refused_projection_shapes_order_correctly(string shape)
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;
        await using var session = theStore.QuerySession();
        var q = session.Query<OrderKeyProjDoc>();

        switch (shape)
        {
            case "anonymous_type":
                var expectedA = Docs.Select(x => new { x.Number, x.Name }).OrderBy(a => a.Name).ToList();
                var actualA = await q.Select(x => new { x.Number, x.Name }).OrderBy(a => a.Name).ToListAsync(token);
                actualA.ShouldBe(expectedA);
                break;
            case "same_type_document":
                var expectedD = Docs.Select(x => x.Inner!).OrderBy(i => i.Number).Select(i => i.Name).ToList();
                var actualD = await q.Select(x => x.Inner!).OrderBy(i => i.Number).ToListAsync(token);
                actualD.Select(i => i.Name).ShouldBe(expectedD);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(shape));
        }
    }

    // marten#5955 "after_select_many": gh-1016 translates SelectMany over a value collection, so the ordering of
    // its elements is ported too
    [Theory]
    [InlineData("negation")]
    [InlineData("modulo_then_value")]
    [InlineData("value_descending")]
    public async Task ordering_the_values_a_select_many_flattens(string shape)
    {
        var parents = Enumerable.Range(1, 4)
            .Select(i => new OrderKeyProjChildParent { Id = Guid.NewGuid(), Values = [i * 3, 10 - i, i % 2] }).ToArray();
        await using (var writer = theStore.LightweightSession())
        {
            writer.Store(parents);
            await writer.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        Func<IQueryable<OrderKeyProjChildParent>, IQueryable<int>> query = shape switch
        {
            "negation" => q => q.SelectMany(x => x.Values).OrderBy(v => -v),
            "modulo_then_value" => q => q.SelectMany(x => x.Values).OrderBy(v => v % 3).ThenBy(v => v),
            "value_descending" => q => q.SelectMany(x => x.Values).OrderByDescending(v => v),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        await using var session = theStore.QuerySession();
        var actual = await query(session.Query<OrderKeyProjChildParent>()).ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(query(parents.AsQueryable()).ToList());
    }
}
