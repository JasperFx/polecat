using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class string_querying : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice Smith" });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob Johnson" });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "ALICE Jones" });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "" });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = null });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task string_contains()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name!.Contains("Smith"))
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice Smith");
    }

    [Fact]
    public async Task string_starts_with()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name!.StartsWith("Ali"))
            .ToListAsync();

        // SQL Server default collation is case-insensitive, so "ALICE Jones" may match too
        results.ShouldContain(r => r.Name == "Alice Smith");
    }

    [Fact]
    public async Task string_ends_with()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name!.EndsWith("Johnson"))
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob Johnson");
    }

    [Fact]
    public async Task string_is_null_or_empty()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => string.IsNullOrEmpty(x.Name))
            .ToListAsync();

        results.Count.ShouldBe(2); // empty string and null
    }

    [Fact]
    public async Task string_case_insensitive_equals()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name!.Equals("alice smith", StringComparison.OrdinalIgnoreCase))
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice Smith");
    }
}
