using JasperFx.Events;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#712 / jasperfx#930: <c>FetchManyForWriting</c> costs a fixed TWO round trips however many
///     streams it is given.
/// </summary>
/// <remarks>
///     The semantics — one handle per id in order, each keeping its own starting version, a version-0
///     handle for a stream that does not exist, and <see cref="ArgumentException" /> on a repeated id —
///     are pinned by the shared <c>FetchForWritingCompliance</c> and <c>StringStreamIdentityCompliance</c>
///     suites, which deliberately pin the semantics and <b>not</b> the round-trip count. The count is the
///     entire reason a store overrides the contract's default, so it is pinned here, where
///     <see cref="IQuerySession.RequestCount" /> can see it. Without the override these assertions read
///     2N instead of 2.
/// </remarks>
[Collection("integration")]
public class fetch_many_for_writing_round_trip_tests : IntegrationContext
{
    public fetch_many_for_writing_round_trip_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<Guid> aQuestAsync(string name)
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted(name),
            new MembersJoined(1, "Shire", ["Frodo"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        return streamId;
    }

    [Fact]
    public async Task eight_streams_cost_the_same_two_round_trips_as_one()
    {
        var ids = new List<Guid>();
        for (var i = 0; i < 8; i++)
        {
            ids.Add(await aQuestAsync($"Quest {i}"));
        }

        await using var session = theStore.LightweightSession();
        var streams = await session.Events.FetchManyForWriting<QuestAggregate>(ids,
            TestContext.Current.CancellationToken);

        streams.Count.ShouldBe(8);
        streams.Select(x => x.Id).ShouldBe(ids);
        streams.ShouldAllBe(x => x.Aggregate != null && x.StartingVersion == 2);

        // One batch for the versions, one for the events. The contract's unbatched default would be 16.
        session.RequestCount.ShouldBe(2);
    }

    [Fact]
    public async Task a_stream_that_does_not_exist_yet_costs_no_extra_round_trip()
    {
        var existing = await aQuestAsync("Real");
        var missing = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        var streams = await session.Events.FetchManyForWriting<QuestAggregate>([missing, existing],
            TestContext.Current.CancellationToken);

        streams[0].Aggregate.ShouldBeNull();
        streams[0].StartingVersion.ShouldBe(0);
        streams[1].Aggregate.ShouldNotBeNull();
        streams[1].StartingVersion.ShouldBe(2);

        session.RequestCount.ShouldBe(2);
    }

    [Fact]
    public async Task no_ids_reads_nothing_at_all()
    {
        await using var session = theStore.LightweightSession();
        var streams = await session.Events.FetchManyForWriting<QuestAggregate>(Array.Empty<Guid>(),
            TestContext.Current.CancellationToken);

        streams.ShouldBeEmpty();
        session.RequestCount.ShouldBe(0);
    }

    [Fact]
    public async Task every_id_missing_still_reads_only_the_versions()
    {
        await using var session = theStore.LightweightSession();
        var streams = await session.Events.FetchManyForWriting<QuestAggregate>(
            [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()], TestContext.Current.CancellationToken);

        streams.Count.ShouldBe(3);
        streams.ShouldAllBe(x => x.Aggregate == null && x.StartingVersion == 0);

        // The event batch has no items to run, so BatchedQuery.Execute short-circuits.
        session.RequestCount.ShouldBe(1);
    }

    [Fact]
    public async Task appending_to_several_handles_commits_every_stream()
    {
        var first = await aQuestAsync("First");
        var second = await aQuestAsync("Second");

        await using (var session = theStore.LightweightSession())
        {
            var streams = await session.Events.FetchManyForWriting<QuestAggregate>([first, second],
                TestContext.Current.CancellationToken);

            streams[0].AppendOne(new MonsterSlain("Troll", 10));
            streams[1].AppendOne(new MonsterSlain("Orc", 5));
            streams[1].AppendOne(new MonsterSlain("Goblin", 3));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(first, token: TestContext.Current.CancellationToken)).Count.ShouldBe(3);
        (await query.Events.FetchStreamAsync(second, token: TestContext.Current.CancellationToken)).Count.ShouldBe(4);
    }
}
