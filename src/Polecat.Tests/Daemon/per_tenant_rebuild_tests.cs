using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #163 Phase 2 headline — per-tenant rebuild isolation. Under per-tenant event partitioning,
///     rebuilding a projection for one tenant must reset and replay ONLY that tenant: other tenants'
///     projected documents and progression rows stay untouched. Mirrors Marten's
///     use_tenant_partitioned_events_per_tenant_rebuild.
/// </summary>
[Collection("tenant-partitioning")]
public class per_tenant_rebuild_tests : IAsyncLifetime
{
    private const string Schema = "pt_rebuild";
    private static readonly string[] Tenants = ["Red", "Blue", "Green"];

    public async Task InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(Schema);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
        });
    }

    [Fact]
    public async Task rebuilding_one_tenant_leaves_other_tenants_untouched()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var streams = new Dictionary<string, Guid>();

        // Append a distinct quest stream per tenant.
        foreach (var tenant in Tenants)
        {
            var streamId = Guid.NewGuid();
            streams[tenant] = streamId;
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Events.StartStream(streamId,
                new QuestStarted($"{tenant} Quest"),
                new MembersJoined(1, $"{tenant} Town", [$"{tenant}Hero"]));
            await session.SaveChangesAsync();
        }

        var projectionName = store.Options.Projections.All.Single().Name;

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();

        // Build every tenant's snapshot via an explicit per-tenant rebuild.
        foreach (var tenant in Tenants)
        {
            await daemon.RebuildProjectionAsync(projectionName, tenant, CancellationToken.None);
        }

        // All three tenants are correctly projected and isolated.
        foreach (var tenant in Tenants)
        {
            await using var query = store.QuerySession(new SessionOptions { TenantId = tenant });
            var party = await query.LoadAsync<QuestParty>(streams[tenant]);
            party.ShouldNotBeNull();
            party.Name.ShouldBe($"{tenant} Quest");
            party.Members.ShouldBe([$"{tenant}Hero"]);
        }

        // Snapshot the OTHER tenants' progression rows before the targeted rebuild.
        var blueProgressBefore = await ProgressRowsForTenantAsync("Blue");
        var greenProgressBefore = await ProgressRowsForTenantAsync("Green");
        blueProgressBefore.ShouldNotBeEmpty();

        // Rebuild ONLY Red.
        await daemon.RebuildProjectionAsync(projectionName, "Red", CancellationToken.None);

        // Red is still correct (rebuilt from scratch)...
        await using (var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            var red = await redQuery.LoadAsync<QuestParty>(streams["Red"]);
            red.ShouldNotBeNull();
            red.Name.ShouldBe("Red Quest");
        }

        // ...and Blue/Green are completely untouched — both their projected docs and their
        // per-tenant progression rows are exactly as they were before Red's rebuild.
        foreach (var tenant in new[] { "Blue", "Green" })
        {
            await using var query = store.QuerySession(new SessionOptions { TenantId = tenant });
            var party = await query.LoadAsync<QuestParty>(streams[tenant]);
            party.ShouldNotBeNull();
            party.Name.ShouldBe($"{tenant} Quest");
            party.Members.ShouldBe([$"{tenant}Hero"]);
        }

        (await ProgressRowsForTenantAsync("Blue")).ShouldBe(blueProgressBefore);
        (await ProgressRowsForTenantAsync("Green")).ShouldBe(greenProgressBefore);
    }

    // polecat#180 (item 2) — cancelling a per-tenant rebuild must leave pc_event_progression in a
    // consistent state: other tenants untouched, and the cancelled tenant never advanced past the
    // events that exist (progress + projected data are committed atomically per batch).
    [Fact]
    public async Task cancelling_a_per_tenant_rebuild_keeps_progression_consistent()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Red gets a longer stream so its rebuild has real work to interrupt.
        const int redEventCount = 26; // QuestStarted + 25 MembersJoined
        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            var events = new List<object> { new QuestStarted("Red Quest") };
            for (var i = 1; i <= 25; i++) events.Add(new MembersJoined(i, "Town", [$"H{i}"]));
            red.Events.StartStream(Guid.NewGuid(), events.ToArray());
            await red.SaveChangesAsync();
        }

        foreach (var tenant in new[] { "Blue", "Green" })
        {
            await using var s = store.LightweightSession(new SessionOptions { TenantId = tenant });
            s.Events.StartStream(Guid.NewGuid(), new QuestStarted($"{tenant} Quest"));
            await s.SaveChangesAsync();
        }

        var projectionName = store.Options.Projections.All.Single().Name;
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();

        foreach (var tenant in Tenants)
        {
            await daemon.RebuildProjectionAsync(projectionName, tenant, CancellationToken.None);
        }

        var blueBefore = await ProgressRowsForTenantAsync("Blue");
        var greenBefore = await ProgressRowsForTenantAsync("Green");

        // Cancel a rebuild of Red. The token may win the race (rebuild aborts) or lose it (rebuild
        // completes) — either outcome must leave a consistent state, so the test tolerates both.
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(20));
        try
        {
            await daemon.RebuildProjectionAsync(projectionName, "Red", cts.Token);
        }
        catch (OperationCanceledException)
        {
        }

        // Red's cancelled rebuild left the other tenants' progression completely untouched.
        (await ProgressRowsForTenantAsync("Blue")).ShouldBe(blueBefore);
        (await ProgressRowsForTenantAsync("Green")).ShouldBe(greenBefore);

        // Red's progression is never advanced past the events that exist — a cancelled rebuild can only
        // leave a valid intermediate (or fully reset) mark, never an over-advanced one.
        foreach (var seq in (await ProgressRowsForTenantAsync("Red")).Values)
        {
            seq.ShouldBeLessThanOrEqualTo(redEventCount);
        }
    }

    // Progression rows (name -> last_seq_id) whose composed identity targets the given tenant.
    private static async Task<Dictionary<string, long>> ProgressRowsForTenantAsync(string tenantId)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT name, last_seq_id FROM [{Schema}].[pc_event_progression] WHERE name LIKE '%:' + @t";
        cmd.Parameters.AddWithValue("@t", tenantId);
        var rows = new Dictionary<string, long>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows[reader.GetString(0)] = reader.GetInt64(1);
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
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
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
            FROM sys.sequences sq
            JOIN sys.schemas s ON sq.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }
}
