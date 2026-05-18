using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// String-keyed sibling of QuestAggregate. The SG keys the generated evolver on
// the document's `Id` property type, so a Guid-id aggregate can't double-serve
// string-keyed streams under Polecat 4 the way it did under 3.x's FEC fallback.
public partial class StringQuestAggregate
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public List<string> Members { get; } = [];
    public int MonstersSlain { get; set; }

    public static StringQuestAggregate Create(QuestStarted e) => new() { Name = e.Name };
    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
    public void Apply(MonsterSlain e) => MonstersSlain++;
}

[Collection("integration")]
public class always_enforce_consistency_tests : IntegrationContext
{
    public always_enforce_consistency_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task no_events_appended_without_flag_does_not_throw()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"),
            new MembersJoined(1, "Shire", ["Frodo", "Sam"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);

        // Modify the stream in another session
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId, new MonsterSlain("Balrog", 100));
        await session3.SaveChangesAsync();

        // No events appended, no flag set — should NOT throw even though version changed
        await session2.SaveChangesAsync();
    }

    [Fact]
    public async Task always_enforce_consistency_throws_when_version_changed()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"),
            new MembersJoined(1, "Shire", ["Frodo", "Sam"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AlwaysEnforceConsistency = true;

        // Modify the stream in another session
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId, new MonsterSlain("Balrog", 100));
        await session3.SaveChangesAsync();

        // No events appended but flag is set — should throw ConcurrencyException
        var ex = await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            session2.SaveChangesAsync());
    }

    [Fact]
    public async Task always_enforce_consistency_succeeds_when_version_unchanged()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"),
            new MembersJoined(1, "Shire", ["Frodo", "Sam"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AlwaysEnforceConsistency = true;

        // No other modifications — should succeed
        await session2.SaveChangesAsync();
    }

    [Fact]
    public async Task always_enforce_consistency_with_events_still_checks_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AlwaysEnforceConsistency = true;

        // Modify the stream in another session
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId, new MonsterSlain("Troll", 50));
        await session3.SaveChangesAsync();

        // Append events AND flag is set — should still throw because version changed
        stream.AppendOne(new MembersJoined(2, "Rivendell", ["Aragorn"]));
        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            session2.SaveChangesAsync());
    }

    [Fact]
    public async Task always_enforce_consistency_with_string_stream_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "enforce_str";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        var key = "quest-" + Guid.NewGuid().ToString("N");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(key,
            new QuestStarted("Ring Quest"));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<StringQuestAggregate>(key);
        stream.AlwaysEnforceConsistency = true;

        // Modify the stream in another session
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(key, new MonsterSlain("Shelob", 75));
        await session3.SaveChangesAsync();

        // Should throw
        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            session2.SaveChangesAsync());
    }

    [Fact]
    public async Task always_enforce_consistency_stream_not_found_throws()
    {
        var streamId = Guid.NewGuid();

        // Create a fake stream action with an expected version but the stream doesn't exist
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AlwaysEnforceConsistency = true;

        // Stream doesn't exist in DB, expected version is 0 (from FetchForWriting on non-existent)
        // Since ExpectedVersionOnServer is 0 and the stream has no rows, version check should
        // still work — 0 == 0 means no conflict
        await session.SaveChangesAsync();
    }
}
