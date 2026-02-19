using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class linq_extensions : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 25, Color = TargetColor.Red });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob", Age = 35, Color = TargetColor.Green });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Charlie", Age = 30, Color = TargetColor.Blue });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Diana", Age = 40, Color = TargetColor.Red });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task is_one_of_with_params_array()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name!.IsOneOf("Alice", "Charlie"))
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Charlie");
    }

    [Fact]
    public async Task is_one_of_with_list()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var names = new List<string> { "Bob", "Diana" };
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name!.IsOneOf(names))
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Bob");
        results[1].Name.ShouldBe("Diana");
    }

    [Fact]
    public async Task in_extension()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age.In(25, 30))
            .OrderBy(x => x.Age)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Charlie");
    }

    [Fact]
    public async Task is_one_of_with_empty_list()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name!.IsOneOf(Array.Empty<string>()))
            .ToListAsync();

        results.Count.ShouldBe(0);
    }

    [Fact]
    public async Task is_one_of_with_enum()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Color.IsOneOf(TargetColor.Red, TargetColor.Blue))
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(3);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Charlie");
        results[2].Name.ShouldBe("Diana");
    }

    [Fact]
    public async Task any_tenant_queries_across_all_tenants()
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        // Store data under two tenants
        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" }))
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 25 });
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-b" }))
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob", Age = 35 });
            await session.SaveChangesAsync();
        }

        // Normal query should only see current tenant
        await using var queryA = theStore.QuerySession(new SessionOptions { TenantId = "tenant-a" });
        var tenantAResults = await queryA.Query<LinqTarget>().ToListAsync();
        tenantAResults.Count.ShouldBe(1);
        tenantAResults[0].Name.ShouldBe("Alice");

        // AnyTenant should see all tenants
        var allResults = await queryA.Query<LinqTarget>()
            .AnyTenant()
            .OrderBy(x => x.Name)
            .ToListAsync();

        allResults.Count.ShouldBe(2);
        allResults[0].Name.ShouldBe("Alice");
        allResults[1].Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task tenant_is_one_of_queries_specific_tenants()
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        // Store data under three tenants
        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" }))
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice" });
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-b" }))
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Bob" });
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-c" }))
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Charlie" });
            await session.SaveChangesAsync();
        }

        // TenantIsOneOf should only see specified tenants
        await using var query = theStore.QuerySession(new SessionOptions { TenantId = "tenant-a" });
        var results = await query.Query<LinqTarget>()
            .TenantIsOneOf("tenant-a", "tenant-c")
            .OrderBy(x => x.Name)
            .ToListAsync();

        results.Count.ShouldBe(2);
        results[0].Name.ShouldBe("Alice");
        results[1].Name.ShouldBe("Charlie");
    }
}
