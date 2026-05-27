using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class distinct_by_operator : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        // "Alice" appears twice (ages 25 and 40) so DistinctBy(Name) must collapse to one row.
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 25, Score = 9.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob", Age = 35, Score = 8.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Charlie", Age = 30, Score = 7.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 40, Score = 6.0 });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task distinct_by_document_member_returns_one_full_document_per_key()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .DistinctBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Select(x => x.Name).OrderBy(x => x)
            .ShouldBe(["Alice", "Bob", "Charlie"]);
    }

    [Fact]
    public async Task distinct_by_after_anonymous_projection()
    {
        await StoreSeedDataAsync();

        // Mirrors the issue's example: Select(...) then DistinctBy(projectedKey).
        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Select(x => new { x.Name, x.Age })
            .DistinctBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Select(x => x.Name).OrderBy(x => x)
            .ShouldBe(["Alice", "Bob", "Charlie"]);
    }

    [Fact]
    public async Task distinct_by_after_dto_projection()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Select(x => new NameAge { Name = x.Name, Age = x.Age })
            .DistinctBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3);
        results.Select(x => x.Name).OrderBy(x => x)
            .ShouldBe(["Alice", "Bob", "Charlie"]);
    }

    [Fact]
    public async Task distinct_by_applies_where_filter_before_dedup()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age >= 30)
            .Select(x => new { x.Name, x.Age })
            .DistinctBy(x => x.Name)
            .ToListAsync();

        // Age >= 30: Bob(35), Charlie(30), Alice(40) -> distinct Name -> 3
        results.Count.ShouldBe(3);
        results.Select(x => x.Name).OrderBy(x => x)
            .ShouldBe(["Alice", "Bob", "Charlie"]);
    }

    [Fact]
    public async Task order_by_decides_surviving_row_per_key_ascending()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Age)
            .DistinctBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3);
        // The lowest-age row survives per name, and the final list is ordered by age.
        results[0].Name.ShouldBe("Alice");
        results[0].Age.ShouldBe(25);
        results.Select(x => x.Age).ShouldBe([25, 30, 35]);
    }

    [Fact]
    public async Task order_by_descending_decides_surviving_row_per_key()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderByDescending(x => x.Age)
            .DistinctBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3);
        // The highest-age Alice (40) survives this time.
        results.Single(x => x.Name == "Alice").Age.ShouldBe(40);
    }

    [Fact]
    public async Task distinct_by_computed_projection_key_throws_actionable_error()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();

        // "Doubled" is a computed expression, not a member of the document — cannot translate.
        var ex = await Should.ThrowAsync<NotSupportedException>(async () =>
            await query.Query<LinqTarget>()
                .Select(x => new { Doubled = x.Age * 2 })
                .DistinctBy(x => x.Doubled)
                .ToListAsync());

        ex.Message.ShouldContain("DistinctBy");
    }
}
