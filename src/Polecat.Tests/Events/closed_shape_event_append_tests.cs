using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Exceptions;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #273 event-dialect: exercises the closed-shape event append path (the shared
///     Weasel.Storage.EventStorage&lt;TId&gt; hierarchy via SqlServerEventStoreDialect) end-to-end
///     against live SQL Server for start/append/expected-version/collision/metadata/tenancy/tags.
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

    // ---- broader parity coverage (increment 3) ----

    [Fact]
    public async Task multiple_sequential_appends_accumulate_versions_and_sequences()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Start"));
        await theSession.SaveChangesAsync();

        await using var s2 = theStore.LightweightSession();
        s2.Events.Append(streamId, new MembersJoined(1, "T", ["A"]));
        await s2.SaveChangesAsync();

        await using var s3 = theStore.LightweightSession();
        s3.Events.Append(streamId, new MonsterSlain("Orc", 1), new MonsterSlain("Goblin", 2));
        await s3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(4);
        events.Select(e => e.Version).ShouldBe([1, 2, 3, 4]);
        for (var i = 1; i < events.Count; i++)
            events[i].Sequence.ShouldBeGreaterThan(events[i - 1].Sequence);
    }

    [Fact]
    public async Task live_aggregation_rebuilds_state()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"),
            new MembersJoined(1, "Shire", ["Frodo", "Sam"]),
            new MonsterSlain("Balrog", 100));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var quest = await query.Events.AggregateStreamAsync<QuestAggregate>(streamId);

        quest.ShouldNotBeNull();
        quest!.Name.ShouldBe("Ring Quest");
        quest.Members.ShouldBe(["Frodo", "Sam"]);
        quest.MonstersSlain.ShouldBe(1);
    }

    [Fact]
    public async Task inline_projection_snapshots_during_append()
    {
        await ConfigureClosedShape(opts =>
            opts.Projections.Add<SingleStreamProjection<QuestAggregate, Guid>>(ProjectionLifecycle.Inline));

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"),
            new MembersJoined(1, "Shire", ["Frodo", "Sam"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var snapshot = await query.LoadAsync<QuestAggregate>(streamId);
        snapshot.ShouldNotBeNull();
        snapshot!.Name.ShouldBe("Ring Quest");
        snapshot.Members.ShouldBe(["Frodo", "Sam"]);
    }

    [Fact]
    public async Task appending_to_archived_stream_throws()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Start"));
        await theSession.SaveChangesAsync();

        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        await using var appendSession = theStore.LightweightSession();
        appendSession.Events.Append(streamId, new MembersJoined(2, "Cave", ["Bilbo"]));

        var ex = await Should.ThrowAsync<InvalidStreamException>(async () =>
            await appendSession.SaveChangesAsync());
        ex.Message.ShouldContain("archived");
    }

    [Fact]
    public async Task optimistic_concurrency_conflict_throws()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId, new QuestStarted("Start"));
        await theSession.SaveChangesAsync();

        // First writer appends and commits, advancing the version.
        await using var winner = theStore.LightweightSession();
        await winner.Events.AppendOptimistic(streamId, new MembersJoined(1, "T", ["A"]));
        await winner.SaveChangesAsync();

        // Second writer read the stale version and must conflict.
        await using var loser = theStore.LightweightSession();
        loser.Events.Append(streamId, 2, new MonsterSlain("Orc", 1));
        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(async () =>
            await loser.SaveChangesAsync());
    }

    [Fact]
    public async Task fetch_for_writing_round_trip()
    {
        await ConfigureClosedShape();
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"),
            new MembersJoined(1, "Shire", ["Frodo"]));
        await theSession.SaveChangesAsync();

        await using var writer = theStore.LightweightSession();
        var stream = await writer.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.Aggregate!.Name.ShouldBe("Ring Quest");
        stream.AppendOne(new MembersJoined(2, "Rivendell", ["Aragorn"]));
        await writer.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
        events[2].Version.ShouldBe(3);
    }

    // ---- DCB tag writes (increment 4) ----

    [Fact]
    public async Task dcb_tags_are_written_and_queryable()
    {
        await ConfigureClosedShape(opts =>
        {
            opts.Events.RegisterTagType<StudentId>("student");
            opts.Events.RegisterTagType<CourseId>("course");
        });

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.Append(streamId, enrolled);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var byStudent = await session2.Events.QueryByTagsAsync(new EventTagQuery().Or<StudentId>(studentId));
        byStudent.Count.ShouldBe(1);
        byStudent[0].Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Alice");

        var byCourse = await session2.Events.QueryByTagsAsync(new EventTagQuery().Or<CourseId>(courseId));
        byCourse.Count.ShouldBe(1);
    }

    [Fact]
    public async Task dcb_tags_across_multiple_events_and_streams()
    {
        await ConfigureClosedShape(opts =>
        {
            opts.Events.RegisterTagType<StudentId>("student");
            opts.Events.RegisterTagType<CourseId>("course");
        });

        var student1 = new StudentId(Guid.NewGuid());
        var student2 = new StudentId(Guid.NewGuid());
        var course = new CourseId(Guid.NewGuid());

        var e1 = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        e1.WithTag(student1, course);
        theSession.Events.Append(Guid.NewGuid(), e1);

        var e2 = theSession.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        e2.WithTag(student2, course);
        theSession.Events.Append(Guid.NewGuid(), e2);

        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var byCourse = await session2.Events.QueryByTagsAsync(new EventTagQuery().Or<CourseId>(course));
        byCourse.Count.ShouldBe(2);

        var byStudent1 = await session2.Events.QueryByTagsAsync(new EventTagQuery().Or<StudentId>(student1));
        byStudent1.Count.ShouldBe(1);
        byStudent1[0].Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Alice");
    }

    [Fact]
    public async Task dcb_tags_with_conjoined_tenancy_isolate_by_tenant()
    {
        await ConfigureClosedShape(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.RegisterTagType<StudentId>("student");
        });

        var studentId = new StudentId(Guid.NewGuid());

        await using var red = theStore.LightweightSession(new SessionOptions { TenantId = "Red" });
        var redEvent = red.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        redEvent.WithTag(studentId);
        red.Events.Append(Guid.NewGuid(), redEvent);
        await red.SaveChangesAsync();

        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = "Red" });
        (await redQuery.Events.QueryByTagsAsync(new EventTagQuery().Or<StudentId>(studentId))).Count.ShouldBe(1);

        await using var blueQuery = theStore.LightweightSession(new SessionOptions { TenantId = "Blue" });
        (await blueQuery.Events.QueryByTagsAsync(new EventTagQuery().Or<StudentId>(studentId))).Count.ShouldBe(0);
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
