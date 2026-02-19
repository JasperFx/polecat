using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class basic_where_queries : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Han", Age = 35, IsActive = true,
            Color = TargetColor.Red, Price = 100.50m, Score = 9.5, BigNumber = 1_000_000,
            Address = new Address { Street = "123 Main", City = "Coruscant", State = "Core" }
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Luke", Age = 25, IsActive = true,
            Color = TargetColor.Green, Price = 200.75m, Score = 8.0, BigNumber = 2_000_000,
            Address = new Address { Street = "456 Farm", City = "Tatooine", State = "Outer Rim" }
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Leia", Age = 25, IsActive = false,
            Color = TargetColor.Blue, Price = 300.00m, Score = 9.0, BigNumber = 3_000_000,
            Address = new Address { Street = "789 Palace", City = "Alderaan", State = "Core" }
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = null, Age = 900, IsActive = true,
            Color = TargetColor.Green, Price = 0m, Score = 10.0, BigNumber = 900_000_000
        });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task where_string_equals()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Han")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Han");
    }

    [Fact]
    public async Task where_string_not_equals()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name != "Han")
            .ToListAsync();

        results.Count.ShouldBe(2); // Luke and Leia (null name excluded by JSON_VALUE != comparison)
    }

    [Fact]
    public async Task where_int_greater_than()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age > 30)
            .ToListAsync();

        results.Count.ShouldBe(2); // Han (35) and Yoda (900)
    }

    [Fact]
    public async Task where_int_less_than_or_equal()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age <= 25)
            .ToListAsync();

        results.Count.ShouldBe(2); // Luke and Leia
    }

    [Fact]
    public async Task where_boolean_true()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.IsActive)
            .ToListAsync();

        results.Count.ShouldBe(3); // Han, Luke, Yoda
    }

    [Fact]
    public async Task where_boolean_false()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => !x.IsActive)
            .ToListAsync();

        results.Count.ShouldBe(1); // Leia
        results[0].Name.ShouldBe("Leia");
    }

    [Fact]
    public async Task where_boolean_equals_true()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.IsActive == true)
            .ToListAsync();

        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task where_boolean_equals_false()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.IsActive == false)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Leia");
    }

    [Fact]
    public async Task where_null_check()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name == null)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Age.ShouldBe(900);
    }

    [Fact]
    public async Task where_not_null_check()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name != null)
            .ToListAsync();

        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task where_nested_property()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Address!.City == "Tatooine")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Luke");
    }

    [Fact]
    public async Task where_and_condition()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age == 25 && x.IsActive)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Luke");
    }

    [Fact]
    public async Task where_or_condition()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name == "Han" || x.Name == "Leia")
            .ToListAsync();

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task where_decimal_comparison()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Price > 150m)
            .ToListAsync();

        results.Count.ShouldBe(2); // Luke (200.75) and Leia (300.00)
    }

    [Fact]
    public async Task where_double_comparison()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Score >= 9.0)
            .ToListAsync();

        results.Count.ShouldBe(3); // Han (9.5), Leia (9.0), Yoda (10.0)
    }

    [Fact]
    public async Task where_long_comparison()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.BigNumber >= 2_000_000)
            .ToListAsync();

        results.Count.ShouldBe(3); // Luke, Leia, Yoda
    }

    [Fact]
    public async Task where_enum_comparison()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Color == TargetColor.Green)
            .ToListAsync();

        results.Count.ShouldBe(2); // Luke and Yoda
    }

    [Fact]
    public async Task where_with_closure_variable()
    {
        await StoreSeedDataAsync();

        var name = "Luke";
        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name == name)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Luke");
    }

    [Fact]
    public async Task where_by_guid_id()
    {
        ConfigureStore(_ => { });

        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = id, Name = "Specific", Age = 42 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Other", Age = 10 });
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Id == id)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Specific");
    }

    [Fact]
    public async Task query_with_no_where_returns_all()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>().ToListAsync();

        results.Count.ShouldBe(4);
    }

    [Fact]
    public async Task chained_where_clauses()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age >= 25)
            .Where(x => x.IsActive)
            .ToListAsync();

        results.Count.ShouldBe(3); // Han, Luke, Yoda
    }
}
