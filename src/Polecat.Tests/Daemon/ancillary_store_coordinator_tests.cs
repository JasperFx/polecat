using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

public interface ICoordinatedAncillaryStore : IDocumentStore;

public record AncillaryTick;

public class AncillaryTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class AncillaryTallyProjection : SingleStreamProjection<AncillaryTally, Guid>
{
    public override AncillaryTally? Evolve(AncillaryTally? snapshot, Guid id, IEvent e)
    {
        snapshot ??= new AncillaryTally { Id = id };
        if (e.Data is AncillaryTick) snapshot.Count++;
        return snapshot;
    }
}

/// <summary>
///     #847: <c>AddProjectionCoordinator&lt;T&gt;</c> built T's coordinator and then configured the
///     DEFAULT store's <c>AsyncMode</c>, because it called the non-generic <c>ConfigurePolecat</c>. T stayed
///     <see cref="DaemonMode.Disabled" />, whose coordinator has no distributor and starts nothing, so a
///     marked store's projections never ran, while the store nobody asked about took a mode it was never
///     given.
/// </summary>
public class ancillary_store_coordinator_tests
{
    private static IHost BuildHost(DaemonMode mode)
    {
        var schema = "anc_coord_" + Guid.NewGuid().ToString("N")[..8];

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                var polecat = services.AddPolecat(opts =>
                {
                    opts.ConnectionString = ConnectionSource.ConnectionString;
                    opts.DatabaseSchemaName = schema + "_main";
                    opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                });

                services.AddPolecatStore<ICoordinatedAncillaryStore>(opts =>
                {
                    opts.ConnectionString = ConnectionSource.ConnectionString;
                    opts.DatabaseSchemaName = schema;
                    opts.AutoCreateSchemaObjects = AutoCreate.All;
                    opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                    opts.Projections.Add<AncillaryTallyProjection>(ProjectionLifecycle.Async);
                });

                polecat.AddProjectionCoordinator<ICoordinatedAncillaryStore>(mode);
            })
            .Build();
    }

    [Theory]
    [InlineData(DaemonMode.Solo)]
    [InlineData(DaemonMode.HotCold)]
    public void the_mode_lands_on_the_marked_store_and_not_on_the_default_one(DaemonMode mode)
    {
        using var host = BuildHost(mode);

        host.Services.GetRequiredService<ICoordinatedAncillaryStore>().Options.DaemonSettings.AsyncMode
            .ShouldBe(mode);
        host.Services.GetRequiredService<IDocumentStore>().Options.DaemonSettings.AsyncMode
            .ShouldBe(DaemonMode.Disabled);
    }

    [Fact]
    public async Task a_marked_coordinator_runs_its_own_stores_projections()
    {
        var token = TestContext.Current.CancellationToken;

        using var host = BuildHost(DaemonMode.Solo);
        await host.StartAsync(token);

        var store = host.Services.GetRequiredService<ICoordinatedAncillaryStore>();
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(id, new AncillaryTick());
            await session.SaveChangesAsync(token);
        }

        var deadline = DateTimeOffset.UtcNow.Add(15.Seconds());
        while (DateTimeOffset.UtcNow < deadline)
        {
            await using var query = store.QuerySession();
            var tally = await query.LoadAsync<AncillaryTally>(id, token);
            if (tally?.Count == 1)
            {
                await host.StopAsync(token);
                return;
            }

            await Task.Delay(100, token);
        }

        throw new TimeoutException("The marked store's coordinator never ran its projection.");
    }
}
