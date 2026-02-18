using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public class QuestLog
{
    public Guid Id { get; set; }
    public string QuestName { get; set; } = string.Empty;
    public string? Location { get; set; }
    public int MemberCount { get; set; }
}

public class QuestLogProjection : EventProjection
{
    public void Project(QuestStarted e, IDocumentSession ops)
    {
        ops.Store(new QuestLog { Id = Guid.NewGuid(), QuestName = e.Name });
    }
}

public class QuestLogWithLambdaProjection : EventProjection
{
    public QuestLogWithLambdaProjection()
    {
        Project<QuestStarted>((e, ops) =>
        {
            ops.Store(new QuestLog { Id = Guid.NewGuid(), QuestName = e.Name });
        });
    }
}

public class MultiEventQuestLogProjection : EventProjection
{
    // Store a QuestLog keyed by stream ID via event data
    public void Project(QuestStarted e, IDocumentSession ops)
    {
        ops.Store(new QuestLog { Id = Guid.NewGuid(), QuestName = e.Name });
    }

    public void Project(MembersJoined e, IDocumentSession ops)
    {
        ops.Store(new QuestLog
        {
            Id = Guid.NewGuid(),
            QuestName = "join",
            Location = e.Location,
            MemberCount = e.Members.Length
        });
    }
}

[Collection("integration")]
public class event_projection_tests : IntegrationContext
{
    public event_projection_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task event_projection_stores_document_inline()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<QuestLogProjection>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Destroy the Ring"));
        await session.SaveChangesAsync();

        // The QuestLog should have been created with a new Guid
        // We can verify it exists by loading all QuestLogs
        // Since we don't know the exact Id, we use a raw query
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {theStore.Options.DatabaseSchemaName}.pc_doc_questlog;";
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task event_projection_with_lambda()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<QuestLogWithLambdaProjection>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Lambda Quest"));
        await session.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {theStore.Options.DatabaseSchemaName}.pc_doc_questlog;";
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task event_projection_handles_multiple_event_types()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<MultiEventQuestLogProjection>(ProjectionLifecycle.Inline);
        });

        // Count existing rows before our test
        await using var connPre = await OpenConnectionAsync();
        await using var cmdPre = connPre.CreateCommand();
        cmdPre.CommandText = $"""
            IF OBJECT_ID('{theStore.Options.DatabaseSchemaName}.pc_doc_questlog', 'U') IS NOT NULL
                SELECT COUNT(*) FROM {theStore.Options.DatabaseSchemaName}.pc_doc_questlog
            ELSE
                SELECT 0
            """;
        var before = (int)(await cmdPre.ExecuteScalarAsync())!;

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Multi Quest"),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas"]));
        await session.SaveChangesAsync();

        // Should have 2 more QuestLog entries — one from QuestStarted, one from MembersJoined
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {theStore.Options.DatabaseSchemaName}.pc_doc_questlog;";
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        (count - before).ShouldBe(2);
    }

    [Fact]
    public async Task event_projection_works_with_async_daemon()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<QuestLogProjection>(ProjectionLifecycle.Async);
        });

        // Insert events
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Async Quest"));
        await session.SaveChangesAsync();

        // Run daemon
        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        // Verify QuestLog was created
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {theStore.Options.DatabaseSchemaName}.pc_doc_questlog;";
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBeGreaterThan(0);
    }
}
