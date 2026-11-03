using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     The store's tenant partition registries hold caches, so a second copy loses what the first knows.
///     They're built on first use by whichever session gets there, and the final gate caught eight
///     sessions starting at once building two (tenant_partition_stale_ordinal_tests).
/// </summary>
public class tenant_registry_first_use_race_tests
{
    [Fact]
    public async Task concurrent_first_use_builds_each_registry_once()
    {
        for (var round = 0; round < 50; round++)
        {
            var options = new StoreOptions
            {
                ConnectionString = ConnectionSource.ConnectionString,
                DatabaseSchemaName = "registry_race"
            };
            options.Events.TenancyStyle = TenancyStyle.Conjoined;
            options.EventGraph.UseTenantPartitionedEvents = true;

            using var store = new DocumentStore(options);
            var graph = store.Options.EventGraph;

            using var go = new Barrier(16);
            var seen = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(() =>
            {
                go.SignalAndWait(TestContext.Current.CancellationToken);
                return (graph.TenantOrdinals, graph.TenantSequences, graph.TenantPartitionManager);
            }, TestContext.Current.CancellationToken)));

            seen.Select(x => x.TenantOrdinals).Distinct().Count().ShouldBe(1);
            seen.Select(x => x.TenantSequences).Distinct().Count().ShouldBe(1);
            seen.Select(x => x.TenantPartitionManager).Distinct().Count().ShouldBe(1);
        }
    }
}
