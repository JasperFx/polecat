using System.Linq.Expressions;
using System.Text.Json.Serialization;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1017 (marten#5942): a computed top-level Select(), and the operators after it -- Where, OrderBy, Distinct
// and the aggregates -- which read the projected value. Ported from Marten's cases.

internal static class CalcQueries
{
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Runs the query and asserts what it returns.</summary>
    public static async Task ShouldMatchAsync<T>(Func<Task<T>> run, Action<T> assertCorrect) => assertCorrect(await run());

    /// <summary>Runs a block of assertions.</summary>
    public static Task ShouldMatchAsync(Func<Task> run) => run();
}


public enum CalcColour
{
    Red,
    Green,
    Blue
}

public class CalcComputedDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public long Big { get; set; }
    public string Name { get; set; } = "";
    public Guid Key { get; set; }
    public CalcColour Colour { get; set; }
    public List<int> Values { get; set; } = new();
}

// marten#5942: a computed top-level Select() -- x.Number % 3, x.Id.ToString(), (long)x.Number -- failed in Marten
// with an exception from inside the expression tree API. Polecat projects any non-member Select() in memory
// (LinqQueryParser.HandleSelect), so the plain shapes are expected to work; the risk is in the operators AFTER
// the Select(), which Polecat translates as if the projected value were the document or the "data" column.
public class computed_select_tests : OneOffConfigurationsContext
{
    // Lower case only, so text ordering agrees under any collation
    private static readonly string[] Pool = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"];

    private static readonly int[] StorageOrder = [3, 8, 1, 6, 4, 7, 2, 5, 10, 9];

    private static readonly CalcComputedDoc[] Docs = StorageOrder.Select(i => new CalcComputedDoc
    {
        Id = Guid.NewGuid(),
        Number = i,
        Big = i * 10_000_000_000L,
        Name = Pool[i % Pool.Length] + i,
        Key = Guid.NewGuid(),
        Colour = (CalcColour)(i % 3),
        Values = Enumerable.Range(0, i % 3).ToList()
    }).ToArray();

    private static CancellationToken Ct => CalcQueries.Ct;

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        foreach (var doc in Docs) session.Store(doc);
        await session.SaveChangesAsync(Ct);
    }

    private async Task matches<T>(Func<IQueryable<CalcComputedDoc>, IQueryable<T>> query, bool ordered,
        bool countLegs)
    {
        var expected = query(Docs.AsQueryable()).ToList();

        await using var session = theStore.QuerySession();

        var actual = await query(session.Query<CalcComputedDoc>()).ToListAsync(Ct);
        if (ordered)
        {
            actual.ShouldBe(expected);
        }
        else
        {
            actual.ShouldBe(expected, ignoreOrder: true);
        }

        // The count legs of a paged shape exercise marten#5920 rather than #5942, which the paged count probes
        // below cover on their own; they are left out here so a failure is attributed to the right defect.
        if (countLegs)
        {
            (await query(session.Query<CalcComputedDoc>()).CountAsync(Ct)).ShouldBe(expected.Count);
            (await query(session.Query<CalcComputedDoc>()).LongCountAsync(Ct)).ShouldBe(expected.LongCount());
            (await query(session.Query<CalcComputedDoc>()).AnyAsync(Ct)).ShouldBe(expected.Any());
        }

        if (!ordered) return;

        (await query(session.Query<CalcComputedDoc>()).FirstOrDefaultAsync(Ct)).ShouldBe(expected.FirstOrDefault());
        if (expected.Any())
        {
            (await query(session.Query<CalcComputedDoc>()).FirstAsync(Ct)).ShouldBe(expected.First());
        }
    }

    public static TheoryData<string> Combinations =>
    [
        "plain", "unordered", "where_before", "where_after", "order_by_after", "order_by_descending_after",
        "distinct", "take_after", "skip_take_before", "skip_take_before_and_after", "where_after_take", "nothing"
    ];

    private async Task combination<TResult>(string combination, Expression<Func<CalcComputedDoc, TResult>> select,
        Expression<Func<TResult, bool>> after)
    {
        await SeedAsync();

        Task strict(Func<IQueryable<CalcComputedDoc>, IQueryable<TResult>> q, bool ordered, bool countLegs = true)
            => matches(q, ordered, countLegs);

        // Operators after a computed Select() are a feature Marten added in #5942; refusing one clearly is fine
        Task lenient(Func<IQueryable<CalcComputedDoc>, IQueryable<TResult>> q, bool ordered)
            => CalcQueries.ShouldMatchAsync(() => matches(q, ordered, countLegs: true));

        await (combination switch
        {
            "plain" => strict(q => q.OrderBy(x => x.Number).Select(select), true),
            "unordered" => strict(q => q.Select(select), false),
            "where_before" => strict(q => q.Where(x => x.Number > 3).OrderBy(x => x.Number).Select(select), true),
            "where_after" => lenient(q => q.OrderBy(x => x.Number).Select(select).Where(after), true),
            "order_by_after" => lenient(q => q.Select(select).OrderBy(r => r), true),
            "order_by_descending_after" => lenient(q => q.Select(select).OrderByDescending(r => r), true),
            "distinct" => lenient(q => q.Select(select).Distinct(), false),
            "take_after" => strict(q => q.OrderBy(x => x.Number).Select(select).Take(4), true, countLegs: false),
            "skip_take_before" => strict(q => q.OrderBy(x => x.Number).Skip(2).Take(5).Select(select), true,
                countLegs: false),
            "skip_take_before_and_after" => strict(
                q => q.OrderBy(x => x.Number).Skip(2).Take(5).Select(select).Skip(1).Take(2), true, countLegs: false),
            "where_after_take" => lenient(q => q.OrderBy(x => x.Number).Select(select).Where(after).Take(2), true),
            "nothing" => strict(q => q.Where(x => x.Number > 100).OrderBy(x => x.Number).Select(select), true),
            _ => throw new ArgumentOutOfRangeException(nameof(combination))
        });
    }

    // marten#5942
    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_modulo(string c) => combination(c, x => x.Number % 3, m => m == 1);

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_arithmetic(string c) =>
        combination(c, x => x.Number * 2 + 1 - x.Number / 4, m => m > 8);

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_negation(string c) => combination(c, x => -x.Number, m => m < -5);

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_floating_division(string c) =>
        combination(c, x => (double)x.Number / 4, d => d > 1.2);

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_long_arithmetic(string c) =>
        combination(c, x => x.Big - x.Number, l => l > 50_000_000_000L);

    // A widening cast failed in Marten with NotSupportedException: the member underneath was read as an int
    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_widening(string c) =>
        combination(c, x => (long)x.Number * 1_000_000_000L, l => l > 4_000_000_000L);

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_enum_cast(string c) => combination(c, x => (int)x.Colour, i => i > 0);

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_id_to_string(string c) =>
        combination(c, x => x.Id.ToString(), s => s != Docs[0].Id.ToString());

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_guid_member_to_string(string c) =>
        combination(c, x => x.Key.ToString(), s => s.Length == 36);

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_int_to_string(string c) =>
        combination(c, x => x.Number.ToString(), s => s.StartsWith("1"));

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_to_upper(string c) => combination(c, x => x.Name.ToUpper(), s => s.StartsWith("D"));

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_concatenation(string c) =>
        combination(c, x => x.Name + "-" + x.Number, s => s.EndsWith("0"));

    [Theory]
    [MemberData(nameof(Combinations))]
    public Task computed_select_enum_as_int(string c) =>
        combination(c, x => (int)x.Colour + x.Number, i => i % 2 == 0);

    // marten#5942: Polecat's aggregate selector has to be a member (LinqQueryParser.HandleAggregation), so a
    // clear refusal is expected; a raw exception or a wrong value is the bug.
    [Fact]
    public async Task aggregates_of_a_computed_projection()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        await CalcQueries.ShouldMatchAsync(() => session.Query<CalcComputedDoc>().SumAsync(x => x.Number % 3, Ct),
            v => v.ShouldBe(Docs.Sum(x => x.Number % 3)));
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcComputedDoc>().MaxAsync(x => x.Number * 7 % 10, Ct),
            v => v.ShouldBe(Docs.Max(x => x.Number * 7 % 10)));
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcComputedDoc>().MinAsync(x => x.Number - 20, Ct),
            v => v.ShouldBe(Docs.Min(x => x.Number - 20)));
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcComputedDoc>().AverageAsync(x => x.Number * 2, Ct),
            v => v.ShouldBe(Docs.Average(x => x.Number * 2), 0.0000001));
    }

    // marten#5942
    [Fact]
    public async Task single_of_a_computed_projection()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        (await session.Query<CalcComputedDoc>().Where(x => x.Number == 7).Select(x => x.Number * 3).SingleAsync(Ct))
            .ShouldBe(21);
    }

    // marten#5942
    [Fact]
    public async Task computed_projection_with_a_captured_value()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        foreach (var divisor in new[] { 2, 3, 4 })
        {
            (await session.Query<CalcComputedDoc>().OrderBy(x => x.Number).Select(x => x.Number % divisor)
                    .ToListAsync(Ct))
                .ShouldBe(Docs.OrderBy(x => x.Number).Select(x => x.Number % divisor).ToList());
        }
    }

    public static TheoryData<string> UntranslatableShapes =>
    [
        "math_call", "conditional", "substring", "collection_count", "decimal_division", "narrowing", "floating_modulo"
    ];

    // marten#5942: Marten refuses these with BadLinqExpressionException naming the Select(). Polecat computes a
    // non-member Select() in memory, so the correct values are as good an answer as a clear refusal. The bug is
    // an exception from inside the expression tree API, which is what Marten threw before the fix.
    [Theory]
    [MemberData(nameof(UntranslatableShapes))]
    public async Task untranslatable_projections_are_correct(string shape)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var q = session.Query<CalcComputedDoc>().OrderBy(x => x.Number);
        var docs = Docs.OrderBy(x => x.Number).ToList();

        switch (shape)
        {
            case "math_call":
                await CalcQueries.ShouldMatchAsync(() => q.Select(x => Math.Abs(x.Number - 5)).ToListAsync(Ct),
                    r => r.ShouldBe(docs.Select(x => Math.Abs(x.Number - 5)).ToList()));
                break;
            case "conditional":
                await CalcQueries.ShouldMatchAsync(() => q.Select(x => x.Number > 3 ? 1 : 0).ToListAsync(Ct),
                    r => r.ShouldBe(docs.Select(x => x.Number > 3 ? 1 : 0).ToList()));
                break;
            case "substring":
                await CalcQueries.ShouldMatchAsync(() => q.Select(x => x.Name.Substring(1)).ToListAsync(Ct),
                    r => r.ShouldBe(docs.Select(x => x.Name.Substring(1)).ToList()));
                break;
            case "collection_count":
                await CalcQueries.ShouldMatchAsync(() => q.Select(x => x.Values.Count() + 1).ToListAsync(Ct),
                    r => r.ShouldBe(docs.Select(x => x.Values.Count() + 1).ToList()));
                break;
            case "decimal_division":
                await CalcQueries.ShouldMatchAsync(
                    () => q.Select(x => (decimal)x.Number / 3m).ToListAsync(Ct),
                    r => r.ShouldBe(docs.Select(x => (decimal)x.Number / 3m).ToList()));
                break;
            case "narrowing":
                await CalcQueries.ShouldMatchAsync(() => q.Select(x => (short)(x.Number + 1)).ToListAsync(Ct),
                    r => r.ShouldBe(docs.Select(x => (short)(x.Number + 1)).ToList()));
                break;
            case "floating_modulo":
                await CalcQueries.ShouldMatchAsync(() => q.Select(x => (double)x.Number % 2.5).ToListAsync(Ct),
                    r => r.ShouldBe(docs.Select(x => (double)x.Number % 2.5).ToList()));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(shape));
        }
    }

    // marten#5942 "skip_take_before_and_after", isolated from the computed projection: Polecat's HandleSkip and
    // HandleTake assign Statement.Offset/Limit rather than composing them with an earlier page.
    [Fact]
    public async Task paging_twice_composes_the_pages()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        (await session.Query<CalcComputedDoc>().OrderBy(x => x.Number).Skip(2).Take(5).Skip(1).Take(2)
                .Select(x => x.Number).ToListAsync(Ct))
            .ShouldBe([4, 5]);

        (await session.Query<CalcComputedDoc>().OrderBy(x => x.Number).Take(5).Skip(1)
                .Select(x => x.Number).ToListAsync(Ct))
            .ShouldBe([2, 3, 4, 5]);

        (await session.Query<CalcComputedDoc>().OrderBy(x => x.Number).Take(2).Take(5)
                .Select(x => x.Number).ToListAsync(Ct))
            .ShouldBe([1, 2]);
    }
}
