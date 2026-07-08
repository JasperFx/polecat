using JasperFx.Events;
using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #273 event-dialect increment 1: exercises the closed-shape event append path
///     (Events.UseClosedShapeEventStorage) end-to-end against live SQL Server, asserting parity with
///     the bespoke inline path for start/append/expected-version/collision/metadata/tenancy.
/// </summary>
[Collection("integration")]
public class closed_shape_event_append_tests : IntegrationContext
{
    public closed_shape_event_append_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task ConfigureClosedShape(Action<StoreOptions>? extra = null)
    {
        var schemaName = "closed_evt_" + Guid.NewGuid().ToString("N")[..8];
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schemaName;
            opts.Events.UseClosedShapeEventStorage = true;
            extra?.Invoke(opts);
        });
    }

    [Fact]
    public async Task start_stream_writes_events_and_versions_and_sequences()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId,
            new QuestStarted("Destroy the Ring"),
            new MembersJoined(1, "Hobbiton", ["Frodo", "Sam"]),
            new ArrivedAtLocation("Bree", 2));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(3);
        events[0].Data.ShouldBeOfType<QuestStarted>();
        events[1].Data.ShouldBeOfType<MembersJoined>();
        events[2].Data.ShouldBeOfType<ArrivedAtLocation>();

        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
        events[2].Version.ShouldBe(3);

        events[0].Sequence.ShouldBeGreaterThan(0);
        events[1].Sequence.ShouldBeGreaterThan(events[0].Sequence);
        events[2].Sequence.ShouldBeGreaterThan(events[1].Sequence);
    }

    [Fact]
    public async Task sequences_are_assigned_back_onto_the_in_memory_events()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        var action = theSession.Events.StartStream(streamId, new QuestStarted("A"), new MembersJoined(1, "B", ["C"]));
        await theSession.SaveChangesAsync();

        action.Events[0].Sequence.ShouldBeGreaterThan(0);
        action.Events[1].Sequence.ShouldBe(action.Events[0].Sequence + 1);
        action.Events[0].Version.ShouldBe(1);
        action.Events[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task append_to_existing_stream_continues_versions()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Start"));
        await theSession.SaveChangesAsync();

        theSession.Events.Append(streamId, new MembersJoined(1, "Town", ["X"]), new ArrivedAtLocation("Cave", 3));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(3);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
        events[2].Version.ShouldBe(3);
    }

    [Fact]
    public async Task duplicate_start_stream_throws_collision()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("First"));
        await theSession.SaveChangesAsync();

        await using var second = theStore.LightweightSession();
        second.Events.StartStream(streamId, new QuestStarted("Second"));

        await Should.ThrowAsync<ExistingStreamIdCollisionException>(async () =>
            await second.SaveChangesAsync());
    }

    [Fact]
    public async Task expected_version_mismatch_throws()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Start"), new MembersJoined(1, "T", ["A"]));
        await theSession.SaveChangesAsync();

        // Stream is at version 2; asserting an append that expects version 5 should fail.
        await using var session = theStore.LightweightSession();
        session.Events.Append(streamId, 8, new ArrivedAtLocation("X", 1));

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(async () =>
            await session.SaveChangesAsync());
    }

    [Fact]
    public async Task correct_expected_version_appends_successfully()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Start"), new MembersJoined(1, "T", ["A"]));
        await theSession.SaveChangesAsync();

        await using var session = theStore.LightweightSession();
        session.Events.Append(streamId, 3, new ArrivedAtLocation("X", 1));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
        events[2].Version.ShouldBe(3);
    }

    [Fact]
    public async Task string_identified_streams()
    {
        await ConfigureClosedShape(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        var key = "quest/" + Guid.NewGuid().ToString("N")[..8];

        theSession.Events.StartStream(key, new QuestStarted("Start"), new MembersJoined(1, "T", ["A"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(key);
        events.Count.ShouldBe(2);
        events[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task conjoined_tenancy_isolates_streams()
    {
        await ConfigureClosedShape(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);
        var streamId = Guid.NewGuid();

        await using var red = theStore.LightweightSession(new SessionOptions { TenantId = "Red" });
        red.Events.StartStream(streamId, new QuestStarted("Red"), new MembersJoined(1, "R", ["a"]));
        await red.SaveChangesAsync();

        await using var blueQuery = theStore.QuerySession(new SessionOptions { TenantId = "Blue" });
        (await blueQuery.Events.FetchStreamAsync(streamId)).Count.ShouldBe(0);

        await using var redQuery = theStore.QuerySession(new SessionOptions { TenantId = "Red" });
        (await redQuery.Events.FetchStreamAsync(streamId)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task always_enforce_consistency_not_found_expected_zero_succeeds()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        // FetchForWriting on a non-existent stream yields ExpectedVersionOnServer == 0; with
        // AlwaysEnforceConsistency and no events, 0 == 0 (missing) must NOT throw.
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AlwaysEnforceConsistency = true;

        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task always_enforce_consistency_throws_when_version_changed()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Ring Quest"), new MembersJoined(1, "Shire", ["Frodo"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AlwaysEnforceConsistency = true;

        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId, new MonsterSlain("Balrog", 100));
        await session3.SaveChangesAsync();

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(session2.SaveChangesAsync());
    }

    [Fact]
    public async Task always_enforce_consistency_succeeds_when_version_unchanged()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Ring Quest"), new MembersJoined(1, "Shire", ["Frodo"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AlwaysEnforceConsistency = true;

        await session2.SaveChangesAsync();
    }

    [Fact]
    public async Task metadata_columns_are_written()
    {
        await ConfigureClosedShape(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
            opts.Events.EnableHeaders = true;
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.CorrelationId = "corr-1";
        session.CausationId = "cause-1";
        session.SetHeader("color", "red");
        session.Events.StartStream(streamId, new QuestStarted("Start"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
        events[0].CorrelationId.ShouldBe("corr-1");
        events[0].CausationId.ShouldBe("cause-1");
        // Headers deserialize as JsonElement under STJ; compare by text like the bespoke path's tests.
        events[0].Headers!["color"].ToString().ShouldBe("red");
    }
}
