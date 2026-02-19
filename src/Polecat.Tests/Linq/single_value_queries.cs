using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class single_value_queries : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 25, Score = 9.0, BigNumber = 100 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob", Age = 35, Score = 8.0, BigNumber = 200 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Charlie", Age = 30, Score = 7.0, BigNumber = 300 });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task first_async()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .OrderBy(x => x.Name)
            .FirstAsync();

        result.ShouldNotBeNull();
        result.Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task first_or_default_async_with_result()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Bob")
            .FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result.Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task first_or_default_async_no_result()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Nobody")
            .FirstOrDefaultAsync();

        result.ShouldBeNull();
    }

    [Fact]
    public async Task first_async_with_predicate()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .OrderBy(x => x.Name)
            .FirstAsync(x => x.Age > 30);

        result.ShouldNotBeNull();
        result.Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task single_async()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Bob")
            .SingleAsync();

        result.ShouldNotBeNull();
        result.Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task single_or_default_async_no_result()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Nobody")
            .SingleOrDefaultAsync();

        result.ShouldBeNull();
    }

    [Fact]
    public async Task single_async_throws_on_multiple()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await query.Query<LinqTarget>()
                .SingleAsync());
    }

    [Fact]
    public async Task count_async()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Query<LinqTarget>().CountAsync();

        count.ShouldBe(3);
    }

    [Fact]
    public async Task count_async_with_predicate()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Query<LinqTarget>()
            .CountAsync(x => x.Age >= 30);

        count.ShouldBe(2);
    }

    [Fact]
    public async Task long_count_async()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Query<LinqTarget>().LongCountAsync();

        count.ShouldBe(3L);
    }

    [Fact]
    public async Task any_async_true()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>().AnyAsync();

        result.ShouldBeTrue();
    }

    [Fact]
    public async Task any_async_false()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Nobody")
            .AnyAsync();

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task any_async_with_predicate()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .AnyAsync(x => x.Age > 30);

        result.ShouldBeTrue();
    }

    [Fact]
    public async Task sum_async_int()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .SumAsync(x => x.Age);

        result.ShouldBe(90); // 25 + 35 + 30
    }

    [Fact]
    public async Task sum_async_long()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .SumAsync(x => x.BigNumber);

        result.ShouldBe(600L); // 100 + 200 + 300
    }

    [Fact]
    public async Task min_async()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .MinAsync(x => x.Age);

        result.ShouldBe(25);
    }

    [Fact]
    public async Task max_async()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .MaxAsync(x => x.Age);

        result.ShouldBe(35);
    }

    [Fact]
    public async Task average_async()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .AverageAsync(x => x.Age);

        result.ShouldBe(30.0); // (25 + 35 + 30) / 3
    }
}
