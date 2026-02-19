using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class collection_querying : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Alice",
            Tags = ["csharp", "dotnet", "sql"],
            Numbers = [1, 2, 3]
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Bob",
            Tags = ["python", "sql"],
            Numbers = [4, 5]
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Charlie",
            Tags = [],
            Numbers = []
        });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task collection_contains_string()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Tags.Contains("sql"))
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task collection_contains_no_match()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Tags.Contains("ruby"))
            .ToListAsync();

        results.Count.ShouldBe(0);
    }

    [Fact]
    public async Task list_contains_member()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var names = new List<string> { "Alice", "Charlie" };
        var results = await query.Query<LinqTarget>()
            .Where(x => names.Contains(x.Name!))
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Charlie");
    }

    [Fact]
    public async Task enumerable_contains_member()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var names = new[] { "Bob" };
        var results = await query.Query<LinqTarget>()
            .Where(x => names.Contains(x.Name!))
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task is_empty_collection()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Tags.IsEmpty())
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Charlie");
    }

    [Fact]
    public async Task collection_contains_combined_with_other_where()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Tags.Contains("sql") && x.Name == "Alice")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice");
    }
}
