using System.Text.Json;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
namespace Polecat.Tests.ExplorerApi;

/// <summary>
///     Integration coverage for the IEventStore explorer methods added in
///     CritterWatch (issues #43 / #44). Each test exercises one of the
///     diagnostic methods against a real Polecat database.
/// </summary>
[Collection("integration")]
public class event_store_explorer_tests : IntegrationContext
{
    public event_store_explorer_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "explorer_tests";
            opts.Projections.Add<SingleStreamProjection<QuestParty, Guid>>(ProjectionLifecycle.Inline);
        });
        return theStore;
    }

    [Fact]
    public async Task get_recent_streams_returns_summaries_newest_first()
    {
        var store = await CreateStoreAsync();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await using (var s = store.LightweightSession())
        {
            s.Events.StartStream<QuestParty>(firstId, new QuestStarted("first"));
            await s.SaveChangesAsync();
        }

        await using (var s = store.LightweightSession())
        {
            s.Events.StartStream<QuestParty>(secondId, new QuestStarted("second"));
            await s.SaveChangesAsync();
        }

        IEventStore es = store;
        var summaries = await es.GetRecentStreamsAsync(10, CancellationToken.None);

        summaries.ShouldNotBeEmpty();
        var ids = summaries.Select(s => s.StreamId).ToList();
        ids.ShouldContain(firstId.ToString());
        ids.ShouldContain(secondId.ToString());
        // Newest stream should appear before the older one
        ids.IndexOf(secondId.ToString()).ShouldBeLessThan(ids.IndexOf(firstId.ToString()));
    }

    [Fact]
    public async Task read_stream_returns_events_in_version_order()
    {
        var store = await CreateStoreAsync();
        var streamId = Guid.NewGuid();

        await using (var s = store.LightweightSession())
        {
            s.Events.StartStream<QuestParty>(streamId,
                new QuestStarted("explorer"),
                new MembersJoined(1, "Town", new[] { "Frodo" }),
                new MonsterSlain("orc", 5));
            await s.SaveChangesAsync();
        }

        IEventStore es = store;
        var events = new List<EventRecord>();
        await foreach (var record in es.ReadStreamAsync(streamId.ToString(), CancellationToken.None))
        {
            events.Add(record);
        }

        events.Count.ShouldBe(3);
        events[0].StreamVersion.ShouldBe(1);
        events[2].StreamVersion.ShouldBe(3);
        events.Select(e => e.EventTypeName).ShouldContain("quest_started");
    }

    [Fact]
    public async Task get_stream_metadata_returns_full_details()
    {
        var store = await CreateStoreAsync();
        var streamId = Guid.NewGuid();

        await using (var s = store.LightweightSession())
        {
            s.Events.StartStream<QuestParty>(streamId, new QuestStarted("meta"),
                new MembersJoined(1, "T", new[] { "P" }));
            await s.SaveChangesAsync();
        }

        IEventStore es = store;
        var metadata = await es.GetStreamMetadataAsync(streamId.ToString(), CancellationToken.None);

        metadata.ShouldNotBeNull();
        metadata!.StreamId.ShouldBe(streamId.ToString());
        metadata.Version.ShouldBe(2);
        metadata.IsArchived.ShouldBeFalse();
    }

    [Fact]
    public async Task get_stream_metadata_returns_null_for_unknown_stream()
    {
        var store = await CreateStoreAsync();
        IEventStore es = store;
        var metadata = await es.GetStreamMetadataAsync(Guid.NewGuid().ToString(), CancellationToken.None);
        metadata.ShouldBeNull();
    }

    [Fact]
    public async Task query_by_tags_throws_not_supported_for_dcb()
    {
        var store = await CreateStoreAsync();
        IEventStore es = store;
        await Should.ThrowAsync<NotSupportedException>(async () =>
        {
            await foreach (var _ in es.QueryByTagsAsync(
                new Dictionary<string, string> { ["k"] = "v" }, CancellationToken.None))
            {
                // Will throw before yielding
            }
        });
    }

    [Fact]
    public async Task get_projected_state_for_tags_throws_not_supported()
    {
        var store = await CreateStoreAsync();
        IEventStore es = store;
        await Should.ThrowAsync<NotSupportedException>(() =>
            es.GetProjectedStateForTagsAsync("any", new Dictionary<string, string>(), CancellationToken.None));
    }

    [Fact]
    public async Task rehydrate_at_version_returns_state_at_supplied_version()
    {
        var store = await CreateStoreAsync();
        var streamId = Guid.NewGuid();

        await using (var s = store.LightweightSession())
        {
            s.Events.StartStream<QuestParty>(streamId,
                new QuestStarted("rehydrate"),
                new MembersJoined(1, "T", new[] { "Sam" }));
            await s.SaveChangesAsync();
        }

        IEventStore es = store;
        var snapshot = await es.RehydrateAtVersionAsync<QuestParty>(streamId, version: 1, CancellationToken.None);

        snapshot.State.ShouldNotBeNull();
        snapshot.State.Name.ShouldBe("rehydrate");
        snapshot.Version.ShouldBe(1);
        snapshot.EventsApplied.ShouldBe(1);
    }

    [Fact]
    public async Task rehydrate_at_version_by_name_returns_json_state()
    {
        var store = await CreateStoreAsync();
        var streamId = Guid.NewGuid();

        await using (var s = store.LightweightSession())
        {
            s.Events.StartStream<QuestParty>(streamId,
                new QuestStarted("by-name"),
                new MembersJoined(2, "T", new[] { "Merry" }));
            await s.SaveChangesAsync();
        }

        IEventStore es = store;
        var snapshot = await es.RehydrateAtVersionByNameAsync(
            typeof(QuestParty).FullName!, streamId, version: 2, CancellationToken.None);

        snapshot.ShouldNotBeNull();
        snapshot!.Version.ShouldBe(2);
        snapshot.EventsApplied.ShouldBe(2);
        snapshot.State.GetProperty("name").GetString().ShouldBe("by-name");
    }

    [Fact]
    public async Task get_projection_statuses_lists_registered_projections()
    {
        var store = await CreateStoreAsync();
        IEventStore es = store;
        var statuses = await es.GetProjectionStatusesAsync(CancellationToken.None);

        statuses.ShouldNotBeNull();
        statuses.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task try_create_usage_populates_registered_event_types()
    {
        var store = await CreateStoreAsync();

        await using (var s = store.LightweightSession())
        {
            s.Events.StartStream<QuestParty>(Guid.NewGuid(), new QuestStarted("registry"));
            await s.SaveChangesAsync();
        }

        IEventStore es = store;
        var usage = await es.TryCreateUsage(CancellationToken.None);

        usage.ShouldNotBeNull();
        usage!.RegisteredEventTypes.ShouldNotBeEmpty();
        usage.RegisteredEventTypes.Any(e => e.Alias == "quest_started").ShouldBeTrue();
    }

    [Fact]
    public async Task run_projection_async_walks_event_list_step_by_step()
    {
        var store = await CreateStoreAsync();
        IEventStore es = store;

        var streamId = Guid.NewGuid().ToString();
        var events = new List<EventRecord>
        {
            BuildRecord(streamId, "quest_started", new QuestStarted("replay"), 1, 1),
            BuildRecord(streamId, "members_joined", new MembersJoined(1, "Town", new[] { "Pippin" }), 2, 2),
        };

        var timeline = await es.RunProjectionAsync<QuestParty>(
            nameof(QuestParty), streamId, events, startingState: null, CancellationToken.None);

        timeline.Steps.Count.ShouldBe(2);
        timeline.Steps[0].Error.ShouldBeNull();
        timeline.FinalState.ShouldNotBeNull();
        timeline.FinalState!.Name.ShouldBe("replay");
        timeline.FinalState.Members.ShouldContain("Pippin");
    }

    private static readonly JsonSerializerOptions s_camelCase = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static EventRecord BuildRecord<T>(string streamId, string typeName, T data, long sequence, long version)
    {
        var json = JsonSerializer.SerializeToElement(data, s_camelCase);
        return new EventRecord(
            EventId: Guid.NewGuid(),
            Sequence: sequence,
            StreamVersion: version,
            StreamId: streamId,
            EventTypeName: typeName,
            Data: json,
            Metadata: null,
            Timestamp: DateTimeOffset.UtcNow,
            TenantId: "*DEFAULT*",
            Tags: null!);
    }
}
