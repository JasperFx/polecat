using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon.Coordination;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #697's own reported walkthrough, re-measured end to end on the merged fix.
///     <para>
///         Both halves of #697 were proven separately — #699 that the per-tenant high-water readings
///         are right, #700 that the distributor hands out per-tenant shard names. Neither of those is
///         the same claim as "the reported failure no longer happens", which was: append 4, 2 and 1
///         events to three tenants under <c>UseTenantPartitionedEvents</c> with an async projection,
///         wait, and find <c>pc_event_progression</c> EMPTY and zero projected documents. This drives
///         the real daemon and asserts the whole thing.
///     </para>
/// </summary>
[Collection("tenant-partitioning")]
public class gh_697_end_to_end_tests : IAsyncLifetime
{
    private const string Schema = "gh697_e2e";

    /// <summary>
    ///     The issue's own shape: three tenants with different stream counts, so a result cannot come
    ///     from one tenant's work being attributed to another.
    /// </summary>
    private static readonly (string Tenant, int Streams)[] Plan =
        [("tenant-a", 4), ("tenant-b", 2), ("tenant-c", 1)];

    public async ValueTask InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore() => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
        opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
        opts.DaemonSettings.AsyncMode = DaemonMode.HotCold;
    });

    [Fact]
    public async Task the_async_projection_advances_for_every_tenant()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        foreach (var (tenant, streams) in Plan)
        {
            for (var i = 0; i < streams; i++)
            {
                await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
                session.Events.StartStream(Guid.NewGuid(),
                    new QuestStarted($"{tenant}-quest-{i}"),
                    new MembersJoined(1, $"{tenant}-town", [$"{tenant}-hero-{i}"]));
                await session.SaveChangesAsync(token);
            }
        }

        // THE ROUTE MATTERS. The issue's own control was BuildProjectionDaemonAsync() + a manual
        // StartAgentAsync, and that already worked before the fix — JasperFxAsyncDaemon.StartAllAsync
        // fans out per tenant off the DETECTOR (_tenantHighWater != null && Database is
        // ICrossTenantRebuildSource), which Polecat has always satisfied. Measured: a test driving
        // that path passes even with DistributesAgentsPerTenant forced back to false.
        //
        // The route that FAILED is the hosted one — AddAsyncDaemon / ProjectionCoordinator — because
        // it gets its shard names from the distributor, which is what gated on
        // DistributesAgentsPerTenant. So this drives the coordinator.
        var coordinator = new ProjectionCoordinator(store, NullLoggerFactory.Instance);
        await coordinator.StartAsync(token);

        // The reported symptom was that this never happens at all, so a bounded wait IS the assertion:
        // a timeout here is the bug reproducing.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(TimeSpan.FromSeconds(90));

        var expectedTotal = Plan.Sum(x => x.Streams);
        var total = 0;

        try
        {
            while (!cts.IsCancellationRequested && total < expectedTotal)
            {
                total = 0;
                foreach (var (tenant, _) in Plan)
                {
                    await using var query = store.QuerySession(tenant);
                    total += await query.Query<QuestParty>().CountAsync(cts.Token);
                }

                if (total < expectedTotal) await Task.Delay(500, cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // Fall through to the assertion, which reports the count actually reached.
        }

        await coordinator.StopAsync(token);

        total.ShouldBe(expectedTotal,
            "the reported failure was zero projected documents for every tenant");

        // Per tenant, not just in total: a fan-out that started one agent and attributed everything to
        // one tenant would satisfy the total and still be the bug.
        foreach (var (tenant, streams) in Plan)
        {
            await using var query = store.QuerySession(tenant);
            (await query.Query<QuestParty>().CountAsync(token))
                .ShouldBe(streams, $"{tenant} should have exactly its own {streams} projected documents");
        }

        // And pc_event_progression — reported EMPTY — carries a row per tenant shard.
        var progression = await ProgressionRowsAsync();

        progression.ShouldNotBeEmpty("pc_event_progression was empty in the report");

        foreach (var (tenant, streams) in Plan)
        {
            var shardRows = progression
                .Where(r => r.Name.EndsWith(":" + tenant, StringComparison.Ordinal))
                .ToList();

            shardRows.ShouldNotBeEmpty($"no progression row for {tenant}");

            // Two events per stream, and seq_id is per-tenant under this mode, so the tenant's shard
            // should have reached its own event count rather than a store-global number.
            shardRows.Select(r => r.LastSeqId).Max()
                .ShouldBeGreaterThanOrEqualTo(streams * 2, $"{tenant}'s shard did not reach its own events");
        }
    }

    private static async Task<List<(string Name, long LastSeqId)>> ProgressionRowsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT name, last_seq_id FROM [{Schema}].[pc_event_progression];";
        var rows = new List<(string, long)>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add((reader.GetString(0), reader.GetInt64(1)));
        return rows;
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task DropSequencesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP SEQUENCE [' + s.name + '].[' + sq.name + '];'
            FROM sys.sequences sq JOIN sys.schemas s ON sq.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }
}
