using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class PagedOperatorDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
    public string Color { get; set; } = "";
}

/// <summary>
///     gh-996 (marten#5955, and the paging legs of marten#5942): the parser kept one flat statement, so an
///     operator after <c>Take()</c>/<c>Skip()</c> rewrote the statement's own clauses instead of applying to the
///     page. A second <c>Skip</c>/<c>Take</c> replaced the first, and an <c>OrderBy</c> after a <c>Take</c> cleared
///     the ordering that chose the page and took the TOP of the new one: a different page, silently. Every
///     expected value here is LINQ to Objects over the same documents.
/// </summary>
public class operators_after_paging_tests : OneOffConfigurationsContext
{
    private static readonly string[] Colors = ["Red", "Green", "Blue"];

    // Names sort in the reverse of Number, so an ordering by Name is a different order from the page's
    private static readonly PagedOperatorDoc[] Docs = Enumerable.Range(1, 10).Select(i => new PagedOperatorDoc
    {
        Id = Guid.NewGuid(), Number = i, Name = $"n{20 - i:00}", Color = Colors[i % 3]
    }).ToArray();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<IQuerySession> SeededAsync()
    {
        await using (var session = theStore.LightweightSession())
        {
            foreach (var doc in Docs) session.Store(doc);
            await session.SaveChangesAsync(Ct);
        }

        return theStore.QuerySession();
    }

    private async Task numbersShouldMatch(Func<IQueryable<PagedOperatorDoc>, IQueryable<int>> shape)
    {
        await using var session = await SeededAsync();

        var expected = shape(Docs.AsQueryable()).ToList();
        (await shape(session.Query<PagedOperatorDoc>()).ToListAsync(Ct)).ShouldBe(expected);
    }

    public static TheoryData<string> PagingTwice =>
    [
        "skip_take_skip_take", "take_skip", "take_take_wider", "take_take_narrower", "skip_skip", "take_skip_past",
        "skip_take_skip_past", "negative_skip", "negative_take"
    ];

    [Theory]
    [MemberData(nameof(PagingTwice))]
    public Task paging_twice_composes_the_pages(string shape) => numbersShouldMatch(q =>
    {
        var ordered = q.OrderBy(x => x.Number);
        IQueryable<PagedOperatorDoc> paged = shape switch
        {
            "skip_take_skip_take" => ordered.Skip(2).Take(5).Skip(1).Take(2),
            "take_skip" => ordered.Take(5).Skip(1),
            "take_take_wider" => ordered.Take(2).Take(5),
            "take_take_narrower" => ordered.Take(5).Take(2),
            "skip_skip" => ordered.Skip(2).Skip(3),
            "take_skip_past" => ordered.Take(3).Skip(5),
            "skip_take_skip_past" => ordered.Skip(1).Take(3).Skip(3).Take(4),
            "negative_skip" => ordered.Skip(-2).Take(3),
            "negative_take" => ordered.Take(-1),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        return paged.Select(x => x.Number);
    });

    public static TheoryData<string> OrderingAfterThePage =>
    [
        "take_then_reverse", "take_then_other_member", "skip_take_then_other_member", "skip_then_reverse",
        "through_a_scalar_select", "reordered_page_then_paged_again", "reordered_page_then_skip_take",
        "where_before_the_page"
    ];

    [Theory]
    [MemberData(nameof(OrderingAfterThePage))]
    public Task an_ordering_after_a_page_reorders_that_page(string shape) => numbersShouldMatch(q => shape switch
    {
        "take_then_reverse" => q.OrderBy(x => x.Number).Take(4).OrderByDescending(x => x.Number)
            .Select(x => x.Number),
        "take_then_other_member" => q.OrderBy(x => x.Number).Take(4).OrderBy(x => x.Name).Select(x => x.Number),
        "skip_take_then_other_member" => q.OrderBy(x => x.Number).Skip(3).Take(4).OrderByDescending(x => x.Name)
            .Select(x => x.Number),
        "skip_then_reverse" => q.OrderBy(x => x.Number).Skip(7).OrderByDescending(x => x.Number)
            .Select(x => x.Number),
        "through_a_scalar_select" => q.OrderBy(x => x.Number).Take(6).Select(x => x.Number)
            .OrderByDescending(n => n),
        "reordered_page_then_paged_again" => q.OrderBy(x => x.Number).Take(6).OrderByDescending(x => x.Number)
            .Take(2).Select(x => x.Number),
        "reordered_page_then_skip_take" => q.OrderBy(x => x.Number).Take(6).OrderByDescending(x => x.Number)
            .Skip(1).Take(3).Select(x => x.Number),
        "where_before_the_page" => q.Where(x => x.Number > 2).OrderBy(x => x.Number).Take(3)
            .OrderByDescending(x => x.Number).Select(x => x.Number),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    });

    [Fact]
    public Task a_where_after_a_page_filters_that_page() => numbersShouldMatch(q =>
        q.OrderBy(x => x.Number).Take(5).Where(x => x.Number > 3).Select(x => x.Number));

    [Fact]
    public async Task single_values_after_a_reordered_or_filtered_page_read_that_page()
    {
        await using var session = await SeededAsync();
        var docs = Docs.AsQueryable();
        var q = session.Query<PagedOperatorDoc>();

        (await q.OrderBy(x => x.Number).Take(5).Where(x => x.Number > 3).CountAsync(Ct))
            .ShouldBe(docs.OrderBy(x => x.Number).Take(5).Count(x => x.Number > 3));
        (await q.OrderBy(x => x.Number).Take(5).CountAsync(x => x.Number > 3, Ct))
            .ShouldBe(docs.OrderBy(x => x.Number).Take(5).Count(x => x.Number > 3));
        (await q.OrderBy(x => x.Number).Take(5).AnyAsync(x => x.Number > 5, Ct)).ShouldBeFalse();
        (await q.OrderBy(x => x.Number).Take(5).OrderByDescending(x => x.Number).Take(2).CountAsync(Ct))
            .ShouldBe(2);
        (await q.OrderBy(x => x.Number).Take(4).OrderByDescending(x => x.Number).FirstAsync(Ct)).Number
            .ShouldBe(4);
        (await q.OrderBy(x => x.Number).Take(4).OrderByDescending(x => x.Number).SumAsync(x => x.Number, Ct))
            .ShouldBe(1 + 2 + 3 + 4);
    }

    // Last() reversed the statement's ordering and took one row: the last row of the whole query
    [Fact]
    public async Task last_after_a_page_is_the_last_row_of_the_page()
    {
        await using var session = await SeededAsync();
        var ordered = session.Query<PagedOperatorDoc>().OrderBy(x => x.Number);

        (await ordered.Take(3).LastAsync(Ct)).Number.ShouldBe(3);
        (await ordered.Skip(2).Take(3).LastOrDefaultAsync(Ct))!.Number.ShouldBe(5);
        (await ordered.Skip(4).LastAsync(Ct)).Number.ShouldBe(10);
        (await ordered.Take(4).OrderByDescending(x => x.Number).LastAsync(Ct)).Number.ShouldBe(1);
        (await ordered.Take(0).LastOrDefaultAsync(Ct)).ShouldBeNull();
    }

    // Removing duplicates from a page is not paging the distinct values: Numbers 2..4 are three colors, and
    // the distinct colors past the first are two
    [Fact]
    public async Task a_distinct_after_a_page_is_over_that_page()
    {
        await using var session = await SeededAsync();
        var expected = Docs.OrderBy(x => x.Number).Skip(1).Take(3).Select(x => x.Color).Distinct().Count();
        expected.ShouldBe(3);

        var colors = await session.Query<PagedOperatorDoc>().OrderBy(x => x.Number).Skip(1).Take(3)
            .Select(x => x.Color).Distinct().ToListAsync(Ct);
        colors.OrderBy(x => x).ShouldBe(["Blue", "Green", "Red"]);

        (await session.Query<PagedOperatorDoc>().OrderBy(x => x.Number).Skip(1).Take(3)
            .Select(x => x.Color).Distinct().CountAsync(Ct)).ShouldBe(expected);
    }

    // A second page that is itself reordered or filtered would take another level of derived table
    [Fact]
    public async Task a_second_reordered_page_is_refused()
    {
        await using var session = await SeededAsync();
        var twice = session.Query<PagedOperatorDoc>().OrderBy(x => x.Number).Take(6)
            .OrderByDescending(x => x.Number).Take(3);

        await Should.ThrowAsync<BadLinqExpressionException>(() => twice.OrderBy(x => x.Name).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => twice.Where(x => x.Number > 4).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => twice.LastAsync(Ct));
    }

    // The groups of a GroupBy are one statement: an ordering or a filter after their page chose another page
    [Fact]
    public async Task an_ordering_or_filter_after_a_page_of_groups_is_refused()
    {
        await using var session = await SeededAsync();
        var groups = session.Query<PagedOperatorDoc>().GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Count = g.Count() }).OrderBy(x => x.Color).Take(2);

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            groups.OrderByDescending(x => x.Count).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => groups.Where(x => x.Count > 3).ToListAsync(Ct));

        // Paging the groups is still fine
        (await groups.ToListAsync(Ct)).Select(x => x.Color).ShouldBe(["Blue", "Green"]);
    }
}

/// <summary>
///     gh-996: the page is chosen from the rows the query may see, so the scoping an execution path adds after
///     parsing goes inside the derived table rather than filtering the page afterwards.
/// </summary>
public class operators_after_paging_scoping_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task soft_deleted_rows_are_not_in_the_page()
    {
        ConfigureStore(opts => opts.Policies.AllDocumentsSoftDeleted());

        var docs = Enumerable.Range(1, 6)
            .Select(i => new PagedOperatorDoc { Id = Guid.NewGuid(), Number = i, Name = $"n{i}" }).ToArray();

        await using (var session = theStore.LightweightSession())
        {
            foreach (var doc in docs) session.Store(doc);
            await session.SaveChangesAsync(Ct);

            session.Delete(docs[0]);
            session.Delete(docs[1]);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        (await query.Query<PagedOperatorDoc>().OrderBy(x => x.Number).Take(3).OrderByDescending(x => x.Number)
                .Select(x => x.Number).ToListAsync(Ct))
            .ShouldBe([5, 4, 3]);
        (await query.Query<PagedOperatorDoc>().OrderBy(x => x.Number).Take(3).Where(x => x.Number < 5)
                .CountAsync(Ct))
            .ShouldBe(2);
    }
}
