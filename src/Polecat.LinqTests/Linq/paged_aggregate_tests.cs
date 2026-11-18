using JasperFx.Events;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class PagedAggregateDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Color { get; set; } = "";
}

/// <summary>
///     gh-995 (marten#5920): a scalar operator after <c>Take()</c>/<c>Skip()</c> aggregated the whole query and
///     then paged the one row the aggregate returned. <c>Take(3).Count()</c> was the count of the table,
///     <c>Skip(4).Count()</c> read no row at all, <c>Take(0).Any()</c> was true and <c>Take(3).Sum()</c> summed
///     every document. Every expected value here is LINQ to Objects over the same documents.
/// </summary>
public class paged_aggregate_tests : OneOffConfigurationsContext
{
    private static readonly string[] Colors = ["Red", "Green", "Blue"];

    private static readonly PagedAggregateDoc[] Docs = Enumerable.Range(1, 10).Select(i => new PagedAggregateDoc
    {
        Id = Guid.NewGuid(), Number = i, Color = Colors[i % 3]
    }).ToArray();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        foreach (var doc in Docs) session.Store(doc);
        await session.SaveChangesAsync(Ct);
    }

    private async Task countsShouldMatch<T>(Func<IQueryable<PagedAggregateDoc>, IQueryable<T>> shape)
    {
        await SeedAsync();
        var expected = shape(Docs.AsQueryable()).Count();

        await using var session = theStore.QuerySession();

        (await shape(session.Query<PagedAggregateDoc>()).CountAsync(Ct)).ShouldBe(expected, "CountAsync");
        (await shape(session.Query<PagedAggregateDoc>()).LongCountAsync(Ct)).ShouldBe(expected, "LongCountAsync");
    }

    public static TheoryData<int> TakeValues() => [0, 1, 3, 10, 25];

    public static TheoryData<int> SkipValues() => [0, 1, 4, 10, 15];

    public static TheoryData<int, int> SkipTakeValues() => new()
    {
        { 0, 3 }, { 4, 3 }, { 8, 5 }, { 10, 3 }, { 15, 2 }, { 2, 25 }, { 3, 0 }
    };

    [Theory]
    [MemberData(nameof(TakeValues))]
    public Task count_after_take(int n) => countsShouldMatch(q => q.Take(n));

    [Theory]
    [MemberData(nameof(SkipValues))]
    public Task count_after_skip(int k) => countsShouldMatch(q => q.Skip(k));

    [Theory]
    [MemberData(nameof(SkipTakeValues))]
    public Task count_after_skip_then_take(int k, int n) => countsShouldMatch(q => q.Skip(k).Take(n));

    [Theory]
    [MemberData(nameof(SkipTakeValues))]
    public Task count_after_order_by_skip_take(int k, int n) =>
        countsShouldMatch(q => q.OrderByDescending(x => x.Number).Skip(k).Take(n));

    [Theory]
    [MemberData(nameof(SkipTakeValues))]
    public Task count_after_where_then_skip_take(int k, int n) =>
        countsShouldMatch(q => q.Where(x => x.Number > 3).OrderBy(x => x.Number).Skip(k).Take(n));

    [Theory]
    [MemberData(nameof(SkipTakeValues))]
    public Task count_after_select_scalar_then_skip_take(int k, int n) =>
        countsShouldMatch(q => q.OrderBy(x => x.Number).Select(x => x.Number).Skip(k).Take(n));

    // A computed projection, which Polecat projects in memory, then paged
    [Theory]
    [MemberData(nameof(TakeValues))]
    public Task count_after_computed_select_then_take(int n) =>
        countsShouldMatch(q => q.OrderBy(x => x.Number).Select(x => x.Number * 2).Take(n));

    // SELECT DISTINCT refused the (SELECT NULL) ordering an OFFSET needs, and FETCH NEXT 0 ROWS is refused
    // outright
    [Theory]
    [MemberData(nameof(SkipTakeValues))]
    public Task count_after_distinct_then_skip_take(int k, int n) =>
        countsShouldMatch(q => q.Select(x => x.Color).Distinct().Skip(k).Take(n));

    [Theory]
    [MemberData(nameof(SkipTakeValues))]
    public async Task any_after_skip_take(int k, int n)
    {
        await SeedAsync();
        var expected = Docs.AsQueryable().Skip(k).Take(n).Any();

        await using var session = theStore.QuerySession();
        (await session.Query<PagedAggregateDoc>().Skip(k).Take(n).AnyAsync(Ct)).ShouldBe(expected);
    }

    // marten#5920: Take(0).Any() widened TOP(0) to TOP(1)
    [Fact]
    public async Task any_after_take_zero_is_false()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        (await session.Query<PagedAggregateDoc>().Take(0).AnyAsync(Ct)).ShouldBeFalse();
        (await session.Query<PagedAggregateDoc>().Where(x => x.Number > 0).Take(0).AnyAsync(x => x.Number > 1, Ct))
            .ShouldBeFalse();
    }

    // Beyond marten#5920, which fixed counts only: Sum/Min/Max/Average took the same path
    [Fact]
    public async Task aggregates_after_take_or_skip_aggregate_the_page()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var ordered = session.Query<PagedAggregateDoc>().OrderBy(x => x.Number);

        (await ordered.Take(3).SumAsync(x => x.Number, Ct)).ShouldBe(6);
        (await ordered.Take(3).MaxAsync(x => x.Number, Ct)).ShouldBe(3);
        (await ordered.Skip(8).MinAsync(x => x.Number, Ct)).ShouldBe(9);
        (await ordered.Take(4).AverageAsync(x => x.Number, Ct)).ShouldBe(2.5, 0.0000001);
        (await ordered.Skip(2).Take(3).SumAsync(x => x.Number, Ct)).ShouldBe(3 + 4 + 5);
        (await ordered.Skip(20).SumAsync(x => x.Number, Ct)).ShouldBe(0);
        (await session.Query<PagedAggregateDoc>().OrderByDescending(x => x.Number).Take(2)
            .MinAsync(x => x.Number, Ct)).ShouldBe(9);
    }

    // Unpaged, the aggregate is still the select list: nothing about these changed
    [Fact]
    public async Task unpaged_aggregates_are_unchanged()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        (await session.Query<PagedAggregateDoc>().OrderBy(x => x.Number).CountAsync(Ct)).ShouldBe(10);
        (await session.Query<PagedAggregateDoc>().SumAsync(x => x.Number, Ct)).ShouldBe(55);
        (await session.Query<PagedAggregateDoc>().Where(x => x.Number > 20).AnyAsync(Ct)).ShouldBeFalse();
    }

    // The same narrowing for the operators that read one row: Take(1).Single() fetched two rows and threw,
    // and Take(0).First() fetched one
    [Fact]
    public async Task first_and_single_after_take_read_the_page()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var ordered = session.Query<PagedAggregateDoc>().OrderBy(x => x.Number);

        (await ordered.Take(1).SingleAsync(Ct)).Number.ShouldBe(1);
        (await ordered.Skip(4).Take(1).SingleOrDefaultAsync(Ct))!.Number.ShouldBe(5);
        (await ordered.Take(0).FirstOrDefaultAsync(Ct)).ShouldBeNull();
        (await ordered.Skip(3).Take(0).FirstOrDefaultAsync(Ct)).ShouldBeNull();
        await Should.ThrowAsync<InvalidOperationException>(() => ordered.Take(0).FirstAsync(Ct));
    }

    // The event store's provider applies single-value operators the same way
    [Fact]
    public async Task event_counts_after_take_or_skip_count_the_page()
    {
        await theStore.Advanced.CleanAllEventDataAsync(Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(),
                Enumerable.Range(1, 6).Select(i => (object)new PagedAggregateEvent(i)).ToArray());
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = query.Events.QueryAllRawEvents().OrderBy(x => x.Sequence);

        (await events.Take(4).CountAsync(Ct)).ShouldBe(4);
        (await events.Skip(4).CountAsync(Ct)).ShouldBe(2);
        (await events.Skip(2).Take(0).CountAsync(Ct)).ShouldBe(0);
        (await events.Take(0).AnyAsync(Ct)).ShouldBeFalse();
        (await events.Take(3).MaxAsync(x => x.Version, Ct)).ShouldBe(3);
    }

    public record PagedAggregateEvent(int Number);
}
