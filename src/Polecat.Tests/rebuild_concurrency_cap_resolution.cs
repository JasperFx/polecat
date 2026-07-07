using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;

namespace Polecat.Tests;

/// <summary>
/// jasperfx#420 / marten#4710 companion: resolution of the per-database rebuild
/// concurrency cap surfaced through <c>IEventStore.MaxConcurrentRebuildsPerDatabase</c>.
/// Pure unit tests — the pool-size derivation parses the connection string via
/// <see cref="SqlConnectionStringBuilder"/> without opening a connection.
/// </summary>
public class rebuild_concurrency_cap_resolution
{
    private const string DummyConnectionString =
        "Server=localhost;Database=rebuild_cap;Integrated Security=true;TrustServerCertificate=true";

    private static DocumentStore buildStore(string connectionString, Action<StoreOptions>? configure = null)
    {
        var options = new StoreOptions
        {
            ConnectionString = connectionString,
            AutoCreateSchemaObjects = AutoCreate.None,
        };
        configure?.Invoke(options);
        return new DocumentStore(options);
    }

    [Fact]
    public void configured_value_wins_over_derived_default()
    {
        using var store = buildStore(DummyConnectionString,
            opts => opts.DaemonSettings.MaxConcurrentRebuildsPerDatabase = 3);
        ((IEventStore)store).MaxConcurrentRebuildsPerDatabase.ShouldBe(3);
    }

    [Fact]
    public void non_positive_configured_value_disables_the_cap()
    {
        using var store = buildStore(DummyConnectionString,
            opts => opts.DaemonSettings.MaxConcurrentRebuildsPerDatabase = 0);
        ((IEventStore)store).MaxConcurrentRebuildsPerDatabase.ShouldBeNull();
    }

    [Fact]
    public void derived_default_is_pool_size_over_eight_with_floor_of_one()
    {
        var connectionString = new SqlConnectionStringBuilder(DummyConnectionString)
        {
            MaxPoolSize = 64
        }.ConnectionString;

        using var store = buildStore(connectionString);
        ((IEventStore)store).MaxConcurrentRebuildsPerDatabase.ShouldBe(8);
    }

    [Fact]
    public void derived_default_floors_at_one_for_tiny_pools()
    {
        var connectionString = new SqlConnectionStringBuilder(DummyConnectionString)
        {
            MaxPoolSize = 5
        }.ConnectionString;

        using var store = buildStore(connectionString);
        ((IEventStore)store).MaxConcurrentRebuildsPerDatabase.ShouldBe(1);
    }

    [Fact]
    public async Task usage_descriptor_carries_the_effective_cap()
    {
        // jasperfx#434: CritterWatch#309's rebuild dispatcher reads the effective cap
        // off the EventStoreUsage descriptor rather than guessing.
        using var store = buildStore(DummyConnectionString,
            opts => opts.DaemonSettings.MaxConcurrentRebuildsPerDatabase = 6);

        var usage = await ((IEventStore)store).TryCreateUsage(CancellationToken.None);

        usage.ShouldNotBeNull();
        usage!.MaxConcurrentRebuildsPerDatabase.ShouldBe(6);
    }
}
