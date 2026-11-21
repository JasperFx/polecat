using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Polecat;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

public record WeighInTicketed(string Ticket);

/// <summary>A <see cref="WeighIn" /> with a natural key, which a poisoned <see cref="Weighed" /> parks too.</summary>
public partial class TicketedWeighIn
{
    public Guid Id { get; set; }

    [NaturalKey]
    public string Ticket { get; set; } = string.Empty;

    public int Count { get; set; }

    public static TicketedWeighIn Create(IEvent<WeighInStarted> e) => new() { Id = e.StreamId };

    [NaturalKeySource]
    public void Apply(WeighInTicketed e) => Ticket = e.Ticket;

    public void Apply(Weighed e)
    {
        if (e.Poison) throw new InvalidOperationException("Can't weigh this one");
        Count++;
    }
}

/// <summary>
///     #1117 (marten#6132): the parked identities health check lists the parked identities themselves, at most ten per
///     projection, each as <c>id (natural key)</c> when its aggregate has a natural key. The counts and the status are
///     what they were.
/// </summary>
public class parked_identities_health_check_detail
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(60);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<IHost> aHostAsync(string schema)
    {
        var host = await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                {
                    opts.ConnectionString = ConnectionSource.ConnectionString;
                    opts.DatabaseSchemaName = schema;
                    opts.AutoCreateSchemaObjects = AutoCreate.All;
                    opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                    opts.Projections.Add<WeighInProjection>(ProjectionLifecycle.Async);
                    opts.Projections.Snapshot<TicketedWeighIn>(SnapshotLifecycle.Async);
                }).AddAsyncDaemon(DaemonMode.Solo);
                services.AddHealthChecks().AddPolecatParkedIdentitiesHealthCheck();
            })
            .StartAsync(Ct);

        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return host;
    }

    private static async Task<Guid> aPoisonedWeighInAsync(IDocumentStore store, string? ticket)
    {
        var stream = Guid.NewGuid();
        await using var session = store.LightweightSession();

        var events = new List<object> { new WeighInStarted() };
        if (ticket != null) events.Add(new WeighInTicketed(ticket));
        events.Add(new Weighed(true));

        session.Events.StartStream<TicketedWeighIn>(stream, events.ToArray());
        await session.SaveChangesAsync(Ct);
        return stream;
    }

    private static async Task<HealthReportEntry> probeAsync(IHost host)
    {
        var report = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync(Ct);
        return report.Entries[nameof(ParkedIdentitiesHealthCheckExtensions.ParkedIdentitiesHealthCheck)];
    }

    [Fact]
    public async Task the_detail_lists_each_parked_identity_with_its_natural_key_when_it_has_one()
    {
        using var host = await aHostAsync("parked_hc_nk_" + Guid.NewGuid().ToString("N")[..8]);
        var store = host.Services.GetRequiredService<IDocumentStore>();

        var ticketed = await aPoisonedWeighInAsync(store, "T-100");
        await ((DocumentStore)store).Database.WaitForNonStaleProjectionDataAsync(Generous);

        var entry = await probeAsync(host);

        // The counts and the status are unchanged: one stream parked by each of the two projections
        entry.Status.ShouldBe(HealthStatus.Degraded);
        var description = entry.Description.ShouldNotBeNull();
        description.ShouldStartWith("2 projection identities are parked: ");

        // The keyed projection shows the stream's natural key. The keyless one shows the bare id.
        var id = ParkedIdentityText.Format(ticketed);
        description.ShouldEndWith($"Parked identities: {nameof(TicketedWeighIn)}: {id} (T-100); {nameof(WeighIn)}: {id}");

        await host.StopAsync(Ct);
    }

    [Fact]
    public async Task a_keyed_stream_without_a_key_shows_the_bare_id()
    {
        using var host = await aHostAsync("parked_hc_nk_" + Guid.NewGuid().ToString("N")[..8]);
        var store = host.Services.GetRequiredService<IDocumentStore>();

        var unticketed = await aPoisonedWeighInAsync(store, null);
        await ((DocumentStore)store).Database.WaitForNonStaleProjectionDataAsync(Generous);

        var description = (await probeAsync(host)).Description.ShouldNotBeNull();
        description.ShouldContain($"{nameof(TicketedWeighIn)}: {ParkedIdentityText.Format(unticketed)}");
        description.ShouldNotContain($"{ParkedIdentityText.Format(unticketed)} (");

        await host.StopAsync(Ct);
    }

    [Fact]
    public async Task the_detail_lists_at_most_ten_identities_of_a_projection()
    {
        using var host = await aHostAsync("parked_hc_nk_" + Guid.NewGuid().ToString("N")[..8]);
        var store = host.Services.GetRequiredService<IDocumentStore>();

        var streams = new List<Guid>();
        for (var i = 0; i < 12; i++)
        {
            streams.Add(await aPoisonedWeighInAsync(store, $"T-{i:D3}"));
        }

        await ((DocumentStore)store).Database.WaitForNonStaleProjectionDataAsync(Generous);

        var entry = await probeAsync(host);
        entry.Status.ShouldBe(HealthStatus.Degraded);

        var description = entry.Description.ShouldNotBeNull();
        description.ShouldStartWith("24 projection identities are parked: ");

        // Ten of each projection's twelve, then how many more there are
        streams.Count(x => description.Contains($"{ParkedIdentityText.Format(x)} (T-")).ShouldBe(10);
        description.ShouldContain($"{nameof(TicketedWeighIn)}: ");
        description.ShouldContain($"{nameof(WeighIn)}: ");
        description.Split(" and 2 more").Length.ShouldBe(3);

        await host.StopAsync(Ct);
    }
}
