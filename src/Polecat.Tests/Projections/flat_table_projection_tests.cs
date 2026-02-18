using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections.Flattened;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public class QuestMetricsProjection : FlatTableProjection
{
    public QuestMetricsProjection() : base("quest_metrics")
    {
        Table.AddColumn("id", "uniqueidentifier").AsPrimaryKey();
        Table.AddColumn("quest_name", "varchar(200)");
        Table.AddColumn("member_count", "int");
        Table.AddColumn("location", "varchar(200)");

        Project<QuestStarted>(map =>
        {
            map.Map(x => x.Name, "quest_name");
            map.SetValue("member_count", 0);
        });

        Project<MembersJoined>(map =>
        {
            map.Increment(x => x.Members.Length, "member_count");
            map.Map(x => x.Location, "location");
        });

        Delete<QuestEnded>();
    }
}

[Collection("integration")]
public class flat_table_projection_tests : IntegrationContext
{
    public flat_table_projection_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithFlatTable(
        ProjectionLifecycle lifecycle = ProjectionLifecycle.Inline)
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<QuestMetricsProjection>(lifecycle);
        });

        // Drop the quest_metrics table if it exists from a previous test run
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "IF OBJECT_ID('dbo.quest_metrics', 'U') IS NOT NULL DROP TABLE dbo.quest_metrics;";
        await cmd.ExecuteNonQueryAsync();

        return theStore;
    }

    private async Task<T?> ReadMetric<T>(string columnName, Guid streamId)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT [{columnName}] FROM [dbo].[quest_metrics] WHERE [id] = @id;";
        cmd.Parameters.AddWithValue("@id", streamId);
        var result = await cmd.ExecuteScalarAsync();
        if (result is null or DBNull) return default;
        return (T)result;
    }

    private async Task<int> CountMetrics()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[quest_metrics];";
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task flat_table_creates_row_on_first_event()
    {
        var store = await CreateStoreWithFlatTable();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Destroy the Ring"));
        await session.SaveChangesAsync();

        var questName = await ReadMetric<string>("quest_name", streamId);
        questName.ShouldBe("Destroy the Ring");

        var memberCount = await ReadMetric<int>("member_count", streamId);
        memberCount.ShouldBe(0);
    }

    [Fact]
    public async Task flat_table_updates_existing_row()
    {
        var store = await CreateStoreWithFlatTable();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Fellowship"),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas", "Gimli"]));
        await session.SaveChangesAsync();

        var questName = await ReadMetric<string>("quest_name", streamId);
        questName.ShouldBe("Fellowship");

        var location = await ReadMetric<string>("location", streamId);
        location.ShouldBe("Rivendell");
    }

    [Fact]
    public async Task flat_table_increments_column()
    {
        var store = await CreateStoreWithFlatTable();

        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("Counter Quest"),
            new MembersJoined(1, "Town", ["A", "B"]));
        await session1.SaveChangesAsync();

        var count1 = await ReadMetric<int>("member_count", streamId);
        count1.ShouldBe(2); // 0 from SetValue + 2 from Increment

        // Append more members
        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new MembersJoined(2, "Forest", ["C", "D", "E"]));
        await session2.SaveChangesAsync();

        var count2 = await ReadMetric<int>("member_count", streamId);
        count2.ShouldBe(5); // 2 + 3
    }

    [Fact]
    public async Task flat_table_deletes_row()
    {
        var store = await CreateStoreWithFlatTable();

        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("Doomed Quest"));
        await session1.SaveChangesAsync();

        // Verify row exists
        var count1 = await CountMetrics();
        count1.ShouldBe(1);

        // End the quest — triggers Delete
        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new QuestEnded("Doomed Quest"));
        await session2.SaveChangesAsync();

        // Row should be gone
        var count2 = await CountMetrics();
        count2.ShouldBe(0);
    }

    [Fact]
    public async Task flat_table_set_value_works()
    {
        var store = await CreateStoreWithFlatTable();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Set Value Quest"));
        await session.SaveChangesAsync();

        // SetValue("member_count", 0) should set the initial value
        var memberCount = await ReadMetric<int>("member_count", streamId);
        memberCount.ShouldBe(0);
    }

    [Fact]
    public async Task flat_table_works_with_async_daemon()
    {
        var store = await CreateStoreWithFlatTable(ProjectionLifecycle.Async);

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Async Flat Quest"),
            new MembersJoined(1, "Castle", ["Knight", "Wizard"]));
        await session.SaveChangesAsync();

        // Run daemon
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        var questName = await ReadMetric<string>("quest_name", streamId);
        questName.ShouldBe("Async Flat Quest");

        var memberCount = await ReadMetric<int>("member_count", streamId);
        memberCount.ShouldBe(2);
    }
}
