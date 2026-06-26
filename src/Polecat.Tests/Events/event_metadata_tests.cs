using JasperFx.Events;
using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     Tests for event metadata: correlation/causation IDs, headers, sequence capture,
///     and concurrency edge cases. Ported from Marten's event metadata test patterns.
/// </summary>
public class event_metadata_tests : OneOffConfigurationsContext
{
    private async Task ConfigureAndApply(Action<StoreOptions> configure)
    {
        ConfigureStore(configure);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    // ===== Correlation / Causation ID tests =====

    [Fact]
    public async Task session_correlation_id_propagates_to_events()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableCorrelationId = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.CorrelationId = "my-correlation-123";
        session.Events.StartStream(streamId, new QuestStarted("Correlated Quest"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(1);
        events[0].CorrelationId.ShouldBe("my-correlation-123");
    }

    [Fact]
    public async Task session_causation_id_propagates_to_events()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableCausationId = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.CausationId = "caused-by-command-456";
        session.Events.StartStream(streamId, new QuestStarted("Caused Quest"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(1);
        events[0].CausationId.ShouldBe("caused-by-command-456");
    }

    [Fact]
    public async Task both_correlation_and_causation_ids_propagate()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.CorrelationId = "corr-789";
        session.CausationId = "cause-012";
        session.Events.StartStream(streamId,
            new QuestStarted("Both IDs"),
            new MembersJoined(1, "Town", ["Alice"]));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events[0].CorrelationId.ShouldBe("corr-789");
        events[0].CausationId.ShouldBe("cause-012");
        events[1].CorrelationId.ShouldBe("corr-789");
        events[1].CausationId.ShouldBe("cause-012");
    }

    [Fact]
    public async Task event_level_correlation_id_takes_precedence_over_session()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableCorrelationId = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.CorrelationId = "session-level";

        var action = session.Events.StartStream(streamId, new QuestStarted("Override"));
        // Set event-level correlation directly
        action.Events[0].CorrelationId = "event-level";
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].CorrelationId.ShouldBe("event-level");
    }

    [Fact]
    public async Task null_correlation_id_when_not_set()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableCorrelationId = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new QuestStarted("No Correlation"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].CorrelationId.ShouldBeNull();
    }

    // ===== Headers tests =====

    [Fact]
    public async Task event_headers_round_trip()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableHeaders = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        var action = session.Events.StartStream(streamId, new QuestStarted("Headers Quest"));
        action.Events[0].SetHeader("user", "admin");
        action.Events[0].SetHeader("source", "api");
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].Headers.ShouldNotBeNull();
        events[0].GetHeader("user")!.ToString().ShouldBe("admin");
        events[0].GetHeader("source")!.ToString().ShouldBe("api");
    }

    [Fact]
    public async Task null_headers_when_not_set()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableHeaders = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new QuestStarted("No Headers"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].Headers.ShouldBeNull();
    }

    // ===== UserName (LastModifiedBy) tests (#237) =====

    [Fact]
    public async Task session_last_modified_by_propagates_to_event_user_name()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableUserName = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.LastModifiedBy = "alice@example.com";
        session.Events.StartStream(streamId,
            new QuestStarted("User Quest"),
            new MembersJoined(1, "Town", ["A"]));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events[0].UserName.ShouldBe("alice@example.com");
        events[1].UserName.ShouldBe("alice@example.com");
    }

    [Fact]
    public async Task event_level_user_name_takes_precedence_over_session()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableUserName = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.LastModifiedBy = "session-user";

        var action = session.Events.StartStream(streamId, new QuestStarted("Override"));
        action.Events[0].UserName = "event-user";
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].UserName.ShouldBe("event-user");
    }

    [Fact]
    public async Task null_user_name_when_not_set()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableUserName = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new QuestStarted("No User"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].UserName.ShouldBeNull();
    }

    [Fact]
    public async Task user_name_column_not_created_when_disabled()
    {
        await ConfigureAndApply(_ => { });

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COL_LENGTH('[{_schema()}].[pc_events]', 'user_name')";
        var result = await cmd.ExecuteScalarAsync();
        (result == null || result == DBNull.Value).ShouldBeTrue();
    }

    private string _schema() => GetType().Name.ToLowerInvariant();

    // ===== Sequence / version capture tests =====

    [Fact]
    public async Task event_sequence_assigned_after_save()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        var action = session.Events.StartStream(streamId,
            new QuestStarted("Sequence Quest"),
            new MembersJoined(1, "Town", ["A"]));
        await session.SaveChangesAsync();

        // Verify sequences were assigned (positive, monotonically increasing)
        action.Events[0].Sequence.ShouldBeGreaterThan(0);
        action.Events[1].Sequence.ShouldBeGreaterThan(action.Events[0].Sequence);
    }

    [Fact]
    public async Task event_versions_assigned_correctly_on_start()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        var action = session.Events.StartStream(streamId,
            new QuestStarted("Version Quest"),
            new MembersJoined(1, "Town", ["A"]),
            new ArrivedAtLocation("Castle", 2));
        await session.SaveChangesAsync();

        action.Events[0].Version.ShouldBe(1);
        action.Events[1].Version.ShouldBe(2);
        action.Events[2].Version.ShouldBe(3);
        action.Version.ShouldBe(3);
    }

    [Fact]
    public async Task event_ids_are_unique_guids()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        var action = session.Events.StartStream(streamId,
            new QuestStarted("ID Quest"),
            new MembersJoined(1, "Town", ["A"]));
        await session.SaveChangesAsync();

        action.Events[0].Id.ShouldNotBe(Guid.Empty);
        action.Events[1].Id.ShouldNotBe(Guid.Empty);
        action.Events[0].Id.ShouldNotBe(action.Events[1].Id);
    }

    // ===== Concurrency edge cases =====

    [Fact]
    public async Task concurrent_appends_to_same_stream_with_expected_version_conflict()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId, new QuestStarted("Conflict Quest"));
        await session1.SaveChangesAsync();

        // Two sessions both read version 1 and try to append at version 2
        await using var sessionA = theStore.LightweightSession();
        await using var sessionB = theStore.LightweightSession();

        sessionA.Events.Append(streamId, 2, new MembersJoined(1, "A", ["X"]));
        sessionB.Events.Append(streamId, 2, new MembersJoined(1, "B", ["Y"]));

        // First one succeeds
        await sessionA.SaveChangesAsync();

        // Second one should fail — expected version 2 but now it's already at 2
        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            sessionB.SaveChangesAsync());
    }

    [Fact]
    public async Task start_stream_with_duplicate_id_throws()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId, new QuestStarted("Original"));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.StartStream(streamId, new QuestStarted("Duplicate"));

        await Should.ThrowAsync<ExistingStreamIdCollisionException>(
            session2.SaveChangesAsync());
    }

    [Fact]
    public async Task append_without_expected_version_always_succeeds()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId, new QuestStarted("No Version Check"));
        await session1.SaveChangesAsync();

        // Two appends without expected version — both should succeed
        await using var sessionA = theStore.LightweightSession();
        sessionA.Events.Append(streamId, new MembersJoined(1, "A", ["X"]));
        await sessionA.SaveChangesAsync();

        await using var sessionB = theStore.LightweightSession();
        sessionB.Events.Append(streamId, new MembersJoined(2, "B", ["Y"]));
        await sessionB.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }

    // ===== Stream state tests =====

    [Fact]
    public async Task fetch_stream_state_for_nonexistent_stream_returns_null()
    {
        await ConfigureAndApply(_ => { });

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(Guid.NewGuid());

        state.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_stream_returns_empty_for_nonexistent_stream()
    {
        await ConfigureAndApply(_ => { });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(Guid.NewGuid());

        events.ShouldBeEmpty();
    }

    [Fact]
    public async Task stream_state_has_correct_version_after_multiple_appends()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("Multi Append"),
            new MembersJoined(1, "Town", ["A"]));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId,
            new ArrivedAtLocation("Castle", 2),
            new MonsterSlain("Dragon", 100));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);

        state.ShouldNotBeNull();
        state.Version.ShouldBe(4);
    }

    // ===== String stream key tests =====

    [Fact]
    public async Task start_stream_with_string_key()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.StreamIdentity = JasperFx.Events.StreamIdentity.AsString;
        });

        var streamKey = "quest-" + Guid.NewGuid().ToString("N");

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamKey, new QuestStarted("String Key Quest"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamKey);

        events.Count.ShouldBe(1);
        events[0].StreamKey.ShouldBe(streamKey);
        events[0].Data.ShouldBeOfType<QuestStarted>();
    }

    [Fact]
    public async Task append_to_stream_with_string_key()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.StreamIdentity = JasperFx.Events.StreamIdentity.AsString;
        });

        var streamKey = "quest-" + Guid.NewGuid().ToString("N");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamKey, new QuestStarted("String Append"));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamKey, new MembersJoined(1, "Town", ["A"]));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamKey);
        events.Count.ShouldBe(2);

        var state = await query.Events.FetchStreamStateAsync(streamKey);
        state.ShouldNotBeNull();
        state.Version.ShouldBe(2);
    }

    [Fact]
    public async Task string_key_with_expected_version_concurrency()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.StreamIdentity = JasperFx.Events.StreamIdentity.AsString;
        });

        var streamKey = "quest-" + Guid.NewGuid().ToString("N");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamKey, new QuestStarted("Concurrency"));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamKey, 5, new MembersJoined(1, "Town", ["A"]));

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            session2.SaveChangesAsync());
    }

    // ===== Fetch with version/timestamp filters =====

    [Fact]
    public async Task fetch_stream_up_to_version()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Version Filter"),
            new MembersJoined(1, "Town", ["A"]),
            new ArrivedAtLocation("Castle", 2),
            new MonsterSlain("Dragon", 100));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, version: 2);

        events.Count.ShouldBe(2);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_stream_from_version()
    {
        await ConfigureAndApply(_ => { });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("From Version"),
            new MembersJoined(1, "Town", ["A"]),
            new ArrivedAtLocation("Castle", 2),
            new MonsterSlain("Dragon", 100));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, fromVersion: 3);

        events.Count.ShouldBe(2);
        events[0].Version.ShouldBe(3);
        events[1].Version.ShouldBe(4);
    }

    // ===== All metadata combined =====

    [Fact]
    public async Task all_metadata_columns_enabled()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
            opts.Events.EnableHeaders = true;
            opts.Events.EnableUserName = true;
        });

        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.CorrelationId = "full-meta-corr";
        session.CausationId = "full-meta-cause";
        session.LastModifiedBy = "full-meta-user";
        var action = session.Events.StartStream(streamId, new QuestStarted("Full Metadata"));
        action.Events[0].SetHeader("env", "test");
        action.Events[0].SetHeader("version", "1.0");
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(1);
        events[0].CorrelationId.ShouldBe("full-meta-corr");
        events[0].CausationId.ShouldBe("full-meta-cause");
        events[0].UserName.ShouldBe("full-meta-user");
        events[0].Headers.ShouldNotBeNull();
        events[0].GetHeader("env")!.ToString().ShouldBe("test");
        events[0].GetHeader("version")!.ToString().ShouldBe("1.0");
    }
}
