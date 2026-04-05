using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class use_identity_map_for_aggregates : IntegrationContext
{
    public use_identity_map_for_aggregates(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idmap_agg";
            opts.Projections.UseIdentityMapForAggregates = true;
        });
    }

    [Fact]
    public async Task fetch_latest_returns_cached_instance_after_fetch_for_writing()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Identity Map Quest"),
            new MembersJoined(1, "Town", ["Alpha", "Beta"]));
        await theSession.SaveChangesAsync();

        // Use a new session to test identity map behavior
        await using var session = theStore.LightweightSession();

        // Fetch for writing — this should cache the aggregate
        var stream = await session.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.Name.ShouldBe("Identity Map Quest");

        // FetchLatest should return the cached instance without hitting the database
        var cached = await session.Events.FetchLatest<QuestAggregate>(streamId);
        cached.ShouldNotBeNull();
        cached!.Name.ShouldBe("Identity Map Quest");

        // Should be the exact same object reference (identity map)
        ReferenceEquals(stream.Aggregate, cached).ShouldBeTrue(
            "FetchLatest should return the same instance cached by FetchForWriting when UseIdentityMapForAggregates is enabled");
    }

    [Fact]
    public async Task fetch_latest_does_not_cache_when_optimization_disabled()
    {
        // Create a store WITHOUT the optimization
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            DatabaseSchemaName = "idmap_disabled",
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        await using var store = new DocumentStore(options);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var session1 = store.LightweightSession();
        var streamId = Guid.NewGuid();
        session1.Events.StartStream(streamId,
            new QuestStarted("No Cache Quest"),
            new MembersJoined(1, "Town", ["Gamma"]));
        await session1.SaveChangesAsync();

        await using var session2 = store.LightweightSession();

        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.Aggregate.ShouldNotBeNull();

        var latest = await session2.Events.FetchLatest<QuestAggregate>(streamId);
        latest.ShouldNotBeNull();

        // Without the optimization, these should be different instances
        ReferenceEquals(stream.Aggregate, latest).ShouldBeFalse(
            "FetchLatest should NOT return the cached instance when UseIdentityMapForAggregates is disabled");
    }
}
