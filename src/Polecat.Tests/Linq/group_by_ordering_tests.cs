using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #837 (the twin of marten#5563): an ordering after GroupBy addresses the group or the projected
///     shape rather than the document. It used to be dropped when the select list looked like an
///     aggregate, and to fail with Msg 8127 otherwise.
/// </summary>
public class group_by_ordering_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public class GroupRow
    {
        public string Category { get; set; } = "";
        public int Total { get; set; }
        public int Smallest { get; set; }
    }

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "B", Age = 5, Color = TargetColor.Blue });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "A", Age = 1, Color = TargetColor.Red });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "A", Age = 2, Color = TargetColor.Red });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "A", Age = 3, Color = TargetColor.Green });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "C", Age = 4, Color = TargetColor.Green });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "C", Age = 9, Color = TargetColor.Blue });
        await session.SaveChangesAsync(Token);
    }

    private IQueryable<LinqTarget> Targets()
    {
        var query = theStore.QuerySession();
        AsyncDisposables.Add(query);
        return query.Query<LinqTarget>();
    }

    [Fact]
    public async Task by_the_scalar_it_selected()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name).Select(g => g.Count())
            .OrderByDescending(x => x).ToListAsync(Token);

        rows.ShouldBe([3, 2, 1]);
    }

    [Fact]
    public async Task by_a_scalar_sum()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name).Select(g => g.Sum(x => x.Age))
            .OrderBy(x => x).ToListAsync(Token);

        rows.ShouldBe([5, 6, 13]);
    }

    [Fact]
    public async Task by_the_key_it_selected()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x).ToListAsync(Token);

        rows.ShouldBe(["A", "B", "C"]);
    }

    [Fact]
    public async Task by_the_key_descending()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name).Select(g => g.Key)
            .OrderByDescending(x => x).ToListAsync(Token);

        rows.ShouldBe(["C", "B", "A"]);
    }

    [Fact]
    public async Task by_a_projected_member()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name)
            .Select(g => new { K = g.Key, C = g.Count() })
            .OrderBy(x => x.C).ThenBy(x => x.K).ToListAsync(Token);

        rows.Select(x => x.K).ShouldBe(["B", "C", "A"]);
    }

    [Fact]
    public async Task by_a_renamed_member_of_a_projected_dto()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name)
            .Select(g => new GroupRow { Category = g.Key!, Total = g.Sum(x => x.Age), Smallest = g.Min(x => x.Age) })
            .OrderByDescending(x => x.Total).ToListAsync(Token);

        rows.Select(x => x.Category).ShouldBe(["C", "A", "B"]);
    }

    [Fact]
    public async Task by_the_group_itself_before_the_select()
    {
        await SeedAsync();

        var byKey = await Targets().GroupBy(x => x.Name).OrderByDescending(g => g.Key)
            .Select(g => g.Key).ToListAsync(Token);

        var byCount = await Targets().GroupBy(x => x.Name).OrderBy(g => g.Count())
            .Select(g => g.Key).ToListAsync(Token);

        byKey.ShouldBe(["C", "B", "A"]);
        byCount.ShouldBe(["B", "C", "A"]);
    }

    [Fact]
    public async Task by_an_aggregate_with_a_bound_predicate()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name)
            .Select(g => new { K = g.Key, Over = g.Count(x => x.Age > 1) })
            .OrderByDescending(x => x.Over).ThenBy(x => x.K).ToListAsync(Token);

        rows.Select(x => x.K).ShouldBe(["A", "C", "B"]);
    }

    [Fact]
    public async Task by_a_composite_key_member()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => new { x.Name, x.Color })
            .Select(g => new { g.Key.Name, g.Key.Color, Count = g.Count() })
            .OrderBy(x => x.Name).ThenBy(x => x.Color).ToListAsync(Token);

        rows.Select(x => (x.Name, x.Color)).ShouldBe(
        [
            ("A", TargetColor.Red), ("A", TargetColor.Green), ("B", TargetColor.Blue),
            ("C", TargetColor.Green), ("C", TargetColor.Blue)
        ]);
    }

    [Fact]
    public async Task with_skip_and_take()
    {
        await SeedAsync();

        var page = await Targets().GroupBy(x => x.Name)
            .Select(g => new { K = g.Key, Total = g.Sum(x => x.Age) })
            .OrderByDescending(x => x.Total).Skip(1).Take(1).ToListAsync(Token);

        page.ShouldHaveSingleItem().K.ShouldBe("A");
    }

    [Fact]
    public async Task with_take_alone()
    {
        await SeedAsync();

        var page = await Targets().GroupBy(x => x.Name).Select(g => g.Count())
            .OrderByDescending(x => x).Take(2).ToListAsync(Token);

        page.ShouldBe([3, 2]);
    }

    [Fact]
    public async Task skip_without_an_ordering_pages_by_the_key()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name).Select(g => g.Key).Skip(1).Take(1).ToListAsync(Token);

        rows.ShouldBe(["B"]);
    }

    [Fact]
    public async Task then_by_replaced_by_a_later_order_by()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name).Select(g => g.Key)
            .OrderByDescending(x => x).OrderBy(x => x).ToListAsync(Token);

        rows.ShouldBe(["A", "B", "C"]);
    }

    [Fact]
    public async Task a_member_the_projection_does_not_have_is_refused()
    {
        await SeedAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().GroupBy(x => x.Name)
                .Select(g => new GroupRow { Category = g.Key!, Total = g.Count() })
                .OrderBy(x => x.Smallest).ToListAsync(Token));

        ex.Message.ShouldContain("Smallest");
    }

    [Fact]
    public async Task an_order_by_in_front_of_the_group_by_is_refused()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().OrderBy(x => x.Age).GroupBy(x => x.Name).Select(g => g.Key).ToListAsync(Token));
    }

    [Fact]
    public async Task a_take_in_front_of_the_group_by_is_refused()
    {
        // It would page the groups instead of grouping five rows, which looks like an answer.
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().Take(5).GroupBy(x => x.Name).Select(g => g.Key).ToListAsync(Token));
    }

    // ---- what the fix has to leave alone: an ordering on a real, ungrouped aggregate ----------------

    [Fact]
    public async Task an_ungrouped_count_still_ignores_an_ordering()
    {
        await SeedAsync();

        (await Targets().OrderBy(x => x.Age).CountAsync(Token)).ShouldBe(6);
    }

    [Fact]
    public async Task an_ungrouped_sum_still_ignores_an_ordering()
    {
        await SeedAsync();

        (await Targets().OrderBy(x => x.Age).SumAsync(x => x.Age, Token)).ShouldBe(24);
    }
}
