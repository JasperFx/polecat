using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

[Collection("integration")]
public class non_stale_data_tests : IntegrationContext
{
    public non_stale_data_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task query_for_non_stale_data_returns_results()
    {
        var uniqueColor = $"NonStale_{Guid.NewGuid():N}";
        var target = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 42 };

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Target>()
            .QueryForNonStaleData()
            .Where(t => t.Color == uniqueColor)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Number.ShouldBe(42);
    }

    [Fact]
    public async Task query_for_non_stale_data_with_timeout()
    {
        var uniqueColor = $"NonStaleTimeout_{Guid.NewGuid():N}";
        var target = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 99 };

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Target>()
            .QueryForNonStaleData(TimeSpan.FromSeconds(10))
            .Where(t => t.Color == uniqueColor)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Number.ShouldBe(99);
    }

    [Fact]
    public async Task query_for_non_stale_data_with_count()
    {
        var uniqueColor = $"NonStaleCount_{Guid.NewGuid():N}";

        theSession.Store(new Target { Id = Guid.NewGuid(), Color = uniqueColor });
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = uniqueColor });
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Query<Target>()
            .QueryForNonStaleData()
            .Where(t => t.Color == uniqueColor)
            .CountAsync();

        count.ShouldBe(2);
    }

    [Fact]
    public async Task query_for_non_stale_data_with_first_or_default()
    {
        var uniqueColor = $"NonStaleFirst_{Guid.NewGuid():N}";
        var target = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 77 };

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<Target>()
            .QueryForNonStaleData()
            .Where(t => t.Color == uniqueColor)
            .FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result.Number.ShouldBe(77);
    }
}
