using JasperFx;
using JasperFx.Events;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events.Rewriting;

/// <summary>
///     #1034 (marten#5643) under per-tenant event partitions, where every tenant numbers its events from 1, so the
///     rewrite's keyset has to be (sequence, tenant).
/// </summary>
[Collection("tenant-partitioning")]
public class rewriting_stored_events_per_tenant_partition : IAsyncLifetime
{
    private const string Schema = "pt_rewrite";

    public record ParcelSent(string To);

    private DocumentStore _store = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);

        _store = DocumentStore.For(opts =>
        {
            opts.EventGraph.AddRewriteTransform<ParcelSent>("bang", e => e with { To = e.To + "!" });
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<ParcelSent>();
        });

        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    public ValueTask DisposeAsync()
    {
        _store.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task appendAsync(params string[] tenants)
    {
        await _store.Advanced.AddPolecatManagedTenantsAsync(Ct, tenants);

        foreach (var tenant in tenants)
        {
            await using var session = _store.LightweightSession(tenant);
            session.Events.StartStream(Guid.NewGuid(), new ParcelSent($"{tenant} 1"), new ParcelSent($"{tenant} 2"));
            await session.SaveChangesAsync(Ct);
        }
    }

    private async Task<string[]> destinationsOfAsync(string tenant)
    {
        await using var query = _store.QuerySession(tenant);
        var events = await query.Events.QueryAllRawEvents().ToListAsync(Ct);
        return events.Select(e => ((ParcelSent)e.Data).To).OrderBy(x => x).ToArray();
    }

    [Fact]
    public async Task every_tenants_events_are_rewritten_although_they_share_sequence_numbers()
    {
        string[] tenants = ["alpha", "beta", "gamma"];
        await appendAsync(tenants);

        // One event per page, so every page boundary falls between two tenants' same-numbered events
        var result = await _store.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.Transform<ParcelSent>(e => e with { To = e.To.ToUpperInvariant() });
            x.BatchSize = 1;
        }, Ct);

        result.Databases.Single().Rewritten.ShouldBe(6);

        foreach (var tenant in tenants)
        {
            (await destinationsOfAsync(tenant))
                .ShouldBe([$"{tenant.ToUpperInvariant()} 1", $"{tenant.ToUpperInvariant()} 2"]);
        }
    }

    [Fact]
    public async Task one_tenant_is_rewritten_alone_and_the_others_keep_their_rows()
    {
        await appendAsync("alpha", "beta");

        var result = await _store.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.Transform<ParcelSent>(e => e with { To = e.To.ToUpperInvariant() });
            x.ForTenant("beta");
            x.BatchSize = 1;
        }, Ct);

        result.Databases.Single().Visited.ShouldBe(2);
        result.Databases.Single().Rewritten.ShouldBe(2);
        result.Databases.Single().Cursor.TenantId.ShouldBe("beta");

        (await destinationsOfAsync("alpha")).ShouldBe(["alpha 1", "alpha 2"]);
        (await destinationsOfAsync("beta")).ShouldBe(["BETA 1", "BETA 2"]);
    }

    [Fact]
    public async Task a_rewrite_job_over_the_whole_database_sends_each_event_through_the_transform_once()
    {
        string[] tenants = ["alpha", "beta", "gamma"];
        await appendAsync(tenants);

        // jasperfx#1215: the job's cursor is a sequence alone, which three tenants share here, so a page has to
        // end on a sequence boundary or a resume would apply the transform twice
        var definition = new EventRewriteDefinition { Name = "pt-bang-" + Guid.NewGuid().ToString("N")[..8], Transforms = ["bang"], BatchSize = 1 };
        var pages = new List<EventRewriteProgress>();

        var run = await EventRewriteRun.FromMarksAsync(_store.Database, definition, Ct);
        run.OnPage = (page, _) =>
        {
            pages.Add(page);
            return Task.CompletedTask;
        };

        var total = (await ((IEventStore)_store).RewriteStoredEventsAsync(run, Ct)).Single();

        total.Rewritten.ShouldBe(6);
        total.Completed.ShouldBeTrue();
        pages.Where(x => x.Visited > 0).ShouldAllBe(x => x.Visited == 3);

        // Run again from the stored marks: nothing is left
        var again = await EventRewriteRun.FromMarksAsync(_store.Database, definition, Ct);
        (await ((IEventStore)_store).RewriteStoredEventsAsync(again, Ct)).Single().Visited.ShouldBe(0);

        foreach (var tenant in tenants)
        {
            (await destinationsOfAsync(tenant)).ShouldBe([$"{tenant} 1!", $"{tenant} 2!"]);
        }
    }
}
