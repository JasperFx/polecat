using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests;

/// <summary>
///     Integration coverage for the SQL-backed fields on
///     <c>EventStoreUsage</c> — currently MaxEventSequence, which queries
///     <c>MAX(seq_id) FROM pc_events</c>. The pure-unit variants live in
///     <see cref="document_store_usage_tests"/>; this file exercises the
///     real-database path.
/// </summary>
[Collection("integration")]
public class event_store_usage_integration_tests : IntegrationContext
{
    public event_store_usage_integration_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DELETE FROM [dbo].[pc_events];
            DELETE FROM [dbo].[pc_streams];
            DELETE FROM [dbo].[pc_event_progression];
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task max_event_sequence_matches_highest_persisted_seq_id()
    {
        // Persist a handful of events, then read MAX(seq_id) back through the
        // typed pc_events column.
        for (var i = 0; i < 5; i++)
        {
            var streamId = Guid.NewGuid();
            theSession.Events.StartStream(streamId, new QuestStarted($"Quest {i + 1}"));
            await theSession.SaveChangesAsync();
        }

        long expected;
        await using (var conn = await OpenConnectionAsync())
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT MAX(seq_id) FROM [dbo].[pc_events];";
            expected = (long)(await cmd.ExecuteScalarAsync())!;
        }

        var usage = await ((IEventStore)theStore).TryCreateUsage(CancellationToken.None);

        usage.ShouldNotBeNull();
        usage.MaxEventSequence.ShouldBe(expected);
    }

    [Fact]
    public async Task max_event_sequence_is_null_when_no_events_persisted()
    {
        // Empty pc_events → MAX returns NULL → MaxEventSequence stays null
        // so CritterWatch renders "n/a" rather than 0.
        var usage = await ((IEventStore)theStore).TryCreateUsage(CancellationToken.None);

        usage.ShouldNotBeNull();
        usage.MaxEventSequence.ShouldBeNull();
    }

    [Fact]
    public async Task projection_error_handling_descriptors_mirror_store_options()
    {
        // JasperFx/ProductSupport#3 — the projection error policy needs to ride
        // along on EventStoreUsage so monitoring tools can render the right
        // affordance (DLQ button vs "shard halts on error" indicator) without
        // sniffing into Polecat internals. Asserts the descriptors carry the
        // exact policy values configured on the store, rather than pinning
        // specific defaults (which can drift across JasperFx.Events releases).
        var usage = await ((IEventStore)theStore).TryCreateUsage(CancellationToken.None);

        usage.ShouldNotBeNull();

        // Both descriptors must be populated so monitoring tools can render
        // policy-driven UI on every store.
        usage.ProjectionErrors.ShouldNotBeNull();
        usage.ProjectionRebuildErrors.ShouldNotBeNull();

        // Mirror assertions — the descriptors must exactly reflect the
        // ErrorHandlingOptions on the store, whatever the upstream defaults are.
        var storeErrors = theStore.Options.Projections.Errors;
        usage.ProjectionErrors.SkipApplyErrors.ShouldBe(storeErrors.SkipApplyErrors);
        usage.ProjectionErrors.SkipUnknownEvents.ShouldBe(storeErrors.SkipUnknownEvents);
        usage.ProjectionErrors.SkipSerializationErrors.ShouldBe(storeErrors.SkipSerializationErrors);

        var storeRebuildErrors = theStore.Options.Projections.RebuildErrors;
        usage.ProjectionRebuildErrors.SkipApplyErrors.ShouldBe(storeRebuildErrors.SkipApplyErrors);
        usage.ProjectionRebuildErrors.SkipUnknownEvents.ShouldBe(storeRebuildErrors.SkipUnknownEvents);
        usage.ProjectionRebuildErrors.SkipSerializationErrors.ShouldBe(storeRebuildErrors.SkipSerializationErrors);
    }
}
