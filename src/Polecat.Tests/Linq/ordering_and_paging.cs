using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class ordering_and_paging : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Charlie", Age = 30, Score = 7.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 25, Score = 9.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob", Age = 35, Score = 8.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Diana", Age = 28, Score = 9.0 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Eve", Age = 22, Score = 6.0 });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task order_by_string()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(5);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Bob");
        results[2].Name.ShouldBe("Charlie");
        results[3].Name.ShouldBe("Diana");
        results[4].Name.ShouldBe("Eve");
    }

    [Fact]
    public async Task order_by_descending()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderByDescending(x => x.Age)
            .ToListAsync();

        results.Count.ShouldBe(5);
        results[0].Name.ShouldBe("Bob");    // 35
        results[1].Name.ShouldBe("Charlie"); // 30
        results[2].Name.ShouldBe("Diana");   // 28
        results[3].Name.ShouldBe("Alice");   // 25
        results[4].Name.ShouldBe("Eve");     // 22
    }

    [Fact]
    public async Task order_by_int()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Age)
            .ToListAsync();

        results.Count.ShouldBe(5);
        results[0].Name.ShouldBe("Eve");     // 22
        results[1].Name.ShouldBe("Alice");   // 25
        results[2].Name.ShouldBe("Diana");   // 28
        results[3].Name.ShouldBe("Charlie"); // 30
        results[4].Name.ShouldBe("Bob");     // 35
    }

    [Fact]
    public async Task then_by()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Score)
            .ThenBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(5);
        results[0].Name.ShouldBe("Eve");     // 6.0
        results[1].Name.ShouldBe("Charlie"); // 7.0
        results[2].Name.ShouldBe("Bob");     // 8.0
        // Score 9.0: Alice, Diana (alphabetical by name)
        results[3].Name.ShouldBe("Alice");   // 9.0
        results[4].Name.ShouldBe("Diana");   // 9.0
    }

    [Fact]
    public async Task then_by_descending()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Score)
            .ThenByDescending(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(5);
        results[0].Name.ShouldBe("Eve");     // 6.0
        results[1].Name.ShouldBe("Charlie"); // 7.0
        results[2].Name.ShouldBe("Bob");     // 8.0
        // Score 9.0: Diana, Alice (reverse alphabetical)
        results[3].Name.ShouldBe("Diana");   // 9.0
        results[4].Name.ShouldBe("Alice");   // 9.0
    }

    [Fact]
    public async Task take()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Name)
            .Take(3)
            .ToListAsync();

        results.Count.ShouldBe(3);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Bob");
        results[2].Name.ShouldBe("Charlie");
    }

    [Fact]
    public async Task skip()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Name)
            .Skip(2)
            .ToListAsync();

        results.Count.ShouldBe(3);
        results[0].Name.ShouldBe("Charlie");
        results[1].Name.ShouldBe("Diana");
        results[2].Name.ShouldBe("Eve");
    }

    [Fact]
    public async Task skip_and_take()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .OrderBy(x => x.Name)
            .Skip(1)
            .Take(2)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Bob");
        results[1].Name.ShouldBe("Charlie");
    }

    [Fact]
    public async Task where_with_order_by()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age >= 28)
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3); // Bob(35), Charlie(30), Diana(28)
        results[0].Name.ShouldBe("Bob");
        results[1].Name.ShouldBe("Charlie");
        results[2].Name.ShouldBe("Diana");
    }

    [Fact]
    public async Task where_with_order_by_and_take()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age >= 25)
            .OrderBy(x => x.Age)
            .Take(2)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Alice");   // 25
        results[1].Name.ShouldBe("Diana");   // 28
    }

    [Fact]
    public async Task take_without_order_by()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Take(2)
            .ToListAsync();

        results.Count.ShouldBe(2);
    }
}
