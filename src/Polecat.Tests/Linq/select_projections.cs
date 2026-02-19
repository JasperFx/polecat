using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class NameAge
{
    public string? Name { get; set; }
    public int Age { get; set; }
}

public class select_projections : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 25, Score = 9.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob", Age = 35, Score = 8.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Charlie", Age = 30, Score = 7.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 40, Score = 6.0 });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task select_scalar_string()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Name)
            .Select(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(4);
        results[0].ShouldBe("Alice");
        results[1].ShouldBe("Alice");
        results[2].ShouldBe("Bob");
        results[3].ShouldBe("Charlie");
    }

    [Fact]
    public async Task select_scalar_int()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Age)
            .Select(x => x.Age)
            .ToListAsync();

        results.Count.ShouldBe(4);
        results[0].ShouldBe(25);
        results[1].ShouldBe(30);
        results[2].ShouldBe(35);
        results[3].ShouldBe(40);
    }

    [Fact]
    public async Task select_anonymous_type()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Bob")
            .Select(x => new { x.Name, x.Age })
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob");
        results[0].Age.ShouldBe(35);
    }

    [Fact]
    public async Task select_dto_type()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Bob")
            .Select(x => new NameAge { Name = x.Name, Age = x.Age })
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob");
        results[0].Age.ShouldBe(35);
    }

    [Fact]
    public async Task select_with_where()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age >= 30)
            .Select(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3); // Bob(35), Charlie(30), Alice(40)
    }

    [Fact]
    public async Task distinct_scalar()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Select(x => x.Name)
            .Distinct()
            .OrderBy(x => x)
            .ToListAsync();

        results.Count.ShouldBe(3); // Alice, Bob, Charlie (deduplicated)
        results.ShouldContain("Alice");
        results.ShouldContain("Bob");
        results.ShouldContain("Charlie");
    }
}
