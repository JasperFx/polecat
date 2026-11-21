using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #1116 (marten#5765): the cheap event store columns, the nullable metadata columns on <c>pc_events</c> and
///     the extended columns on <c>pc_event_progression</c>, are created whatever the flags say. Weasel drops a
///     column a store doesn't declare, so two stores or two apps on one schema with different flags used to drop
///     each other's columns. The flags still decide what is written.
/// </summary>
public class always_create_event_store_columns : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly string[] MetadataColumns = ["correlation_id", "causation_id", "headers", "user_name"];

    private static readonly string[] ExtendedProgressionColumns =
    [
        "heartbeat", "agent_status", "pause_reason", "running_on_node", "warning_behind_threshold",
        "critical_behind_threshold", "failure_category", "failure_event_sequence", "failure_event_type",
        "failure_event_tenant_id", "parked_count"
    ];

    private static void columnFlagsOff(StoreOptions opts)
    {
        opts.Events.EnableCorrelationId = false;
        opts.Events.EnableCausationId = false;
        opts.Events.EnableHeaders = false;
        opts.Events.EnableUserName = false;
        opts.Events.EnableExtendedProgressionTracking = false;
    }

    private static void everyFlagOff(StoreOptions opts)
    {
        opts.RestoreV5Defaults();
        columnFlagsOff(opts);
    }

    private async Task<List<string>> columnsAsync(string table)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT c.name FROM sys.columns c
            WHERE c.object_id = OBJECT_ID('[{theStore.Options.DatabaseSchemaName}].[{table}]')
            """;
        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) names.Add(reader.GetString(0));
        return names;
    }

    [Fact]
    public async Task the_columns_exist_with_every_flag_off()
    {
        ConfigureStore(everyFlagOff);
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var events = await columnsAsync("pc_events");
        foreach (var column in MetadataColumns) events.ShouldContain(column);

        var progression = await columnsAsync("pc_event_progression");
        foreach (var column in ExtendedProgressionColumns) progression.ShouldContain(column);

        // Nothing to migrate afterwards: the flags-off store doesn't see them as extras
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(Ct);
    }

    [Fact]
    public async Task a_flags_off_store_drops_nothing_a_flags_on_store_created()
    {
        // The Polecat 6 defaults: every flag on
        ConfigureStore(_ => { });
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.CorrelationId = "corr-1";
            session.Events.StartStream(streamId, new QuestStarted("Shared schema"));
            await session.SaveChangesAsync(Ct);
        }

        // A second store on the same schema with the flags behind these columns off. Not RestoreV5Defaults(),
        // whose archived-stream partitioning changes the table's shape and so stays a real schema difference.
        ConfigureStore(columnFlagsOff);
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(Ct);

        var events = await columnsAsync("pc_events");
        foreach (var column in MetadataColumns) events.ShouldContain(column);

        var progression = await columnsAsync("pc_event_progression");
        foreach (var column in ExtendedProgressionColumns) progression.ShouldContain(column);

        // The flags-off store writes no metadata, and the flags-on store's value is still there
        await using (var session = theStore.LightweightSession())
        {
            session.CorrelationId = "corr-2";
            session.Events.Append(streamId, new MembersJoined(1, "Town", ["Frodo"]));
            await session.SaveChangesAsync(Ct);
        }

        // And the flags-on store still matches the schema
        ConfigureStore(_ => { });
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(Ct);

        await using var query = theStore.QuerySession();
        var stream = await query.Events.FetchStreamAsync(streamId, token: Ct);
        stream.Select(x => x.CorrelationId).ShouldBe(["corr-1", null]);
    }
}
