using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class group_by_operator : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alpha", Age = 10, Color = TargetColor.Blue, Score = 1.5 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alpha", Age = 20, Color = TargetColor.Blue, Score = 2.5 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Beta", Age = 30, Color = TargetColor.Green, Score = 3.5 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Beta", Age = 40, Color = TargetColor.Green, Score = 4.5 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Gamma", Age = 50, Color = TargetColor.Green, Score = 5.5 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Gamma", Age = 60, Color = TargetColor.Red, Score = 6.5 });
        await session.SaveChangesAsync();
    }

    #region sample_polecat_group_by_simple_key_with_count

    [Fact]
    public async Task group_by_simple_key_with_count()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Count = g.Count() })
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Single(x => x.Color == TargetColor.Blue).Count.ShouldBe(2);
        results.Single(x => x.Color == TargetColor.Green).Count.ShouldBe(3);
        results.Single(x => x.Color == TargetColor.Red).Count.ShouldBe(1);
    }

    #endregion

    [Fact]
    public async Task group_by_simple_key_with_sum()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Total = g.Sum(x => x.Age) })
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Single(x => x.Color == TargetColor.Blue).Total.ShouldBe(30);
        results.Single(x => x.Color == TargetColor.Green).Total.ShouldBe(120);
        results.Single(x => x.Color == TargetColor.Red).Total.ShouldBe(60);
    }

    [Fact]
    public async Task group_by_simple_key_with_multiple_aggregates()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => new
            {
                Color = g.Key,
                Count = g.Count(),
                Total = g.Sum(x => x.Age),
                Min = g.Min(x => x.Age),
                Max = g.Max(x => x.Age)
            })
            .ToListAsync();

        results.Count.ShouldBe(3);

        var blue = results.Single(x => x.Color == TargetColor.Blue);
        blue.Count.ShouldBe(2);
        blue.Total.ShouldBe(30);
        blue.Min.ShouldBe(10);
        blue.Max.ShouldBe(20);

        var green = results.Single(x => x.Color == TargetColor.Green);
        green.Count.ShouldBe(3);
        green.Total.ShouldBe(120);
        green.Min.ShouldBe(30);
        green.Max.ShouldBe(50);
    }

    [Fact]
    public async Task group_by_string_key()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Name)
            .Select(g => new { Key = g.Key, Count = g.Count() })
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Single(x => x.Key == "Alpha").Count.ShouldBe(2);
        results.Single(x => x.Key == "Beta").Count.ShouldBe(2);
        results.Single(x => x.Key == "Gamma").Count.ShouldBe(2);
    }

    [Fact]
    public async Task group_by_with_where_before_group()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age > 20)
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Count = g.Count() })
            .ToListAsync();

        // Blue has 10, 20 -> both filtered out
        // Green has 30, 40, 50 -> 3 pass
        // Red has 60 -> 1 passes
        results.Count.ShouldBe(2);
        results.Single(x => x.Color == TargetColor.Green).Count.ShouldBe(3);
        results.Single(x => x.Color == TargetColor.Red).Count.ShouldBe(1);
    }

    [Fact]
    public async Task group_by_with_having()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Where(g => g.Count() > 1)
            .Select(g => new { Color = g.Key, Count = g.Count() })
            .ToListAsync();

        // Blue=2, Green=3, Red=1 -> Red filtered by HAVING
        results.Count.ShouldBe(2);
        results.ShouldNotContain(x => x.Color == TargetColor.Red);
    }

    [Fact]
    public async Task group_by_composite_key()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => new { x.Color, x.Name })
            .Select(g => new { Color = g.Key.Color, Text = g.Key.Name, Count = g.Count() })
            .ToListAsync();

        // Blue+Alpha=2, Green+Beta=2, Green+Gamma=1, Red+Gamma=1
        results.Count.ShouldBe(4);
        results.Single(x => x.Color == TargetColor.Blue && x.Text == "Alpha").Count.ShouldBe(2);
        results.Single(x => x.Color == TargetColor.Green && x.Text == "Beta").Count.ShouldBe(2);
        results.Single(x => x.Color == TargetColor.Green && x.Text == "Gamma").Count.ShouldBe(1);
        results.Single(x => x.Color == TargetColor.Red && x.Text == "Gamma").Count.ShouldBe(1);
    }

    [Fact]
    public async Task group_by_with_average()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Avg = g.Average(x => x.Score) })
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Single(x => x.Color == TargetColor.Blue).Avg.ShouldBe(2.0, tolerance: 0.01);
        results.Single(x => x.Color == TargetColor.Green).Avg.ShouldBe(4.5, tolerance: 0.01);
        results.Single(x => x.Color == TargetColor.Red).Avg.ShouldBe(6.5, tolerance: 0.01);
    }

    [Fact]
    public async Task group_by_select_key_only()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => g.Key)
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.ShouldContain(TargetColor.Blue);
        results.ShouldContain(TargetColor.Green);
        results.ShouldContain(TargetColor.Red);
    }

    [Fact]
    public async Task group_by_with_long_count()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Count = g.LongCount() })
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Single(x => x.Color == TargetColor.Blue).Count.ShouldBe(2L);
        results.Single(x => x.Color == TargetColor.Green).Count.ShouldBe(3L);
    }
}
