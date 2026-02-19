using JasperFx;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Seeding;

[Collection("integration")]
public class initial_data_tests : IntegrationContext
{
    public initial_data_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private static readonly Guid AliceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid BobId = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly Guid TargetId = Guid.Parse("33333333-3333-3333-3333-333333333333");
    private static readonly Guid LambdaId = Guid.Parse("44444444-4444-4444-4444-444444444444");

    [Fact]
    public async Task initial_data_populates_on_activator_start()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "seed_basic";
            opts.ShouldApplyChangesOnStartup = true;
            opts.InitialData.Add(new SeedUsers());
        });

        var activator = new PolecatActivator(theStore);
        await activator.StartAsync(CancellationToken.None);

        await using var query = theStore.QuerySession();
        var user = await query.LoadAsync<User>(AliceId);
        user.ShouldNotBeNull();
        user.FirstName.ShouldBe("Alice");
    }

    [Fact]
    public async Task multiple_initial_data_instances_run_in_order()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "seed_multi";
            opts.ShouldApplyChangesOnStartup = true;
            opts.InitialData.Add(new SeedUsers());
            opts.InitialData.Add(new SeedTargets());
        });

        var activator = new PolecatActivator(theStore);
        await activator.StartAsync(CancellationToken.None);

        await using var query = theStore.QuerySession();
        var user = await query.LoadAsync<User>(AliceId);
        user.ShouldNotBeNull();

        var target = await query.LoadAsync<Target>(TargetId);
        target.ShouldNotBeNull();
        target.Color.ShouldBe("Seeded");
    }

    [Fact]
    public async Task lambda_initial_data()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "seed_lambda";
            opts.ShouldApplyChangesOnStartup = true;
            opts.InitialData.Add(async (store, ct) =>
            {
                await using var session = store.LightweightSession();
                session.Store(new User { Id = LambdaId, FirstName = "Lambda" });
                await session.SaveChangesAsync(ct);
            });
        });

        var activator = new PolecatActivator(theStore);
        await activator.StartAsync(CancellationToken.None);

        await using var query = theStore.QuerySession();
        var user = await query.LoadAsync<User>(LambdaId);
        user.ShouldNotBeNull();
        user.FirstName.ShouldBe("Lambda");
    }

    [Fact]
    public async Task initial_data_runs_even_without_schema_migration()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "seed_no_migrate";
            // ShouldApplyChangesOnStartup defaults to false
            opts.InitialData.Add(new SeedUsers());
        });

        // Manually ensure schema exists
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var activator = new PolecatActivator(theStore);
        await activator.StartAsync(CancellationToken.None);

        await using var query = theStore.QuerySession();
        var user = await query.LoadAsync<User>(AliceId);
        user.ShouldNotBeNull();
    }

    private class SeedUsers : IInitialData
    {
        public async Task Populate(IDocumentStore store, CancellationToken cancellation)
        {
            await using var session = store.LightweightSession();
            session.Store(new User { Id = AliceId, FirstName = "Alice", LastName = "Seed" });
            session.Store(new User { Id = BobId, FirstName = "Bob", LastName = "Seed" });
            await session.SaveChangesAsync(cancellation);
        }
    }

    private class SeedTargets : IInitialData
    {
        public async Task Populate(IDocumentStore store, CancellationToken cancellation)
        {
            await using var session = store.LightweightSession();
            session.Store(new Target { Id = TargetId, Color = "Seeded" });
            await session.SaveChangesAsync(cancellation);
        }
    }
}
