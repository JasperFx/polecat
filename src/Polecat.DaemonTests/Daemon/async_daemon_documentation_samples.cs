using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Linq;

namespace Polecat.Tests.Daemon.DocumentationSamples;

// Compile-only samples for docs/events/projections/async-daemon.md. Nothing here runs as a test; the
// behavior each sample shows is covered by the tests named next to it.
public class async_daemon_documentation_samples
{
    // Covered by Configuration/polecat_6_default_tests.cs, rebuild_dead_letters_tests.cs and
    // dead_letter_count_tests.cs
    public void configure_error_handling(StoreOptions opts)
    {
        #region sample_polecat_daemon_error_handling
        // A continuous run: park the identity that failed (the Polecat 6 default),
        // Pause to stop the shard, or Skip to dead-letter the event and carry on
        opts.Projections.Errors.ApplyErrors = ProjectionErrorAction.Park;
        opts.Projections.Errors.SerializationErrors = ProjectionErrorAction.Park;

        // The parking guard rails, shown with their defaults
        opts.Projections.Errors.MaxParkedIdentitiesPerShard = 1000;
        opts.Projections.Errors.ParkedTripFraction = 0.5;
        opts.Projections.Errors.ParkedTripMinimum = 20;

        // A rebuild has options of its own. It pauses by default, and never parks
        opts.Projections.RebuildErrors.ApplyErrors = ProjectionErrorAction.Skip;
        #endregion
    }

    // Covered by Daemon/rebuild_dead_letters_tests.cs and Daemon/dead_letter_count_tests.cs
    public async Task read_dead_letters(IDocumentStore store, CancellationToken token)
    {
        #region sample_polecat_reading_dead_letters
        await using var session = store.QuerySession();

        // Each skipped event is a DeadLetterEvent document, in pc_doc_deadletterevent
        var deadLetters = await session.Query<DeadLetterEvent>()
            .Where(x => x.ProjectionName == "TripSummary")
            .ToListAsync(token);
        #endregion
    }

    // Covered by Daemon/rebuilding_a_single_stream_beside_a_daemon.cs
    public async Task rebuild_single_stream(DocumentStore store, Guid tripId)
    {
        #region sample_polecat_rebuild_single_stream
        await store.Advanced.RebuildSingleStreamAsync<TripSummary>(tripId);

        // Under conjoined tenancy or a database per tenant
        await store.Advanced.RebuildSingleStreamAsync<TripSummary>(tripId, "tenant-b");
        #endregion
    }

    // Covered by the IdentityRebuildCompliance and MultiStreamParkedIdentityCompliance suites
    // (Polecat.ComplianceTests, waves 25 and 27)
    public async Task rebuild_one_identity(IProjectionDaemon daemon, Guid tripId, Guid invoiceId,
        CancellationToken token)
    {
        #region sample_polecat_rebuild_identity_through_the_daemon
        // A single-stream projection
        var result = await daemon.RebuildIdentityAsync("TripSummary", tenantId: null, identity: tripId, token);

        // The document is now consistent with every event of its stream up to result.BoundCeiling

        // A multi-stream projection grouped with IdentityFromStream<T>(), IdentityFromTag<T>()
        // or Options.UseSliceIndex()
        await daemon.RebuildIdentityAsync("InvoiceTotals", tenantId: null, identity: invoiceId, token);
        #endregion

        #region sample_polecat_rebuild_identity_by_scan
        // Any other multi-stream projection: read every event of its types and keep this identity's
        var progress = new Progress<IdentityScanProgress>(p =>
            Console.WriteLine($"{p.ScannedThrough} of {p.Ceiling}, {p.EventsFound} found"));

        await daemon.RebuildIdentityByScanAsync("Departments", tenantId: null, identity: "engineering", progress,
            token);
        #endregion
    }

    // Covered by the ParkedIdentityCompliance suite (Polecat.ComplianceTests, wave 27):
    // a_failing_identity_parks_and_a_replay_unparks_it, a_discard_dead_letters_what_still_fails_applies_the_rest_and_unparks
    // and replaying_all_and_the_oldest_unparks_in_parked_order
    public async Task replay_and_discard(DocumentStore store, Guid tripId, CancellationToken token)
    {
        #region sample_polecat_replay_and_discard_parked_identities
        var daemon = await store.BuildProjectionDaemonAsync();

        var parked = await daemon.FetchParkedIdentitiesAsync("TripSummary", tenantId: null, token);

        // One identity
        await daemon.ReplayParkedIdentityAsync("TripSummary", null, tripId, token);
        await daemon.DiscardParkedIdentityAsync("TripSummary", null, tripId, token);

        // Every parked identity, or the five parked first
        await daemon.ReplayParkedIdentitiesAsync("TripSummary", null, ParkedSelection.All, token);
        await daemon.ReplayParkedIdentitiesAsync("TripSummary", null, ParkedSelection.OldestFirst(5), token);
        #endregion
    }

    // Covered by ParkedIdentityCompliance.a_durable_request_is_run_by_the_shard_on_its_heartbeat
    public async Task request_a_replay_from_another_node(DocumentStore store, CancellationToken token)
    {
        #region sample_polecat_request_parked_action
        // From a node that doesn't run the shard. The node that does picks it up on its heartbeat
        await store.Database.RequestParkedActionAsync(new ParkedActionRequest
        {
            ProjectionName = "TripSummary",
            Action = ParkedAction.Replay
        }, token);
        #endregion
    }

    // Covered by Configuration/polecat_6_default_tests.cs and
    // Daemon/parked_identities.cs (with_the_parked_identities_turned_off_a_failing_identity_pauses_the_shard)
    public void skip_like_polecat_5(StoreOptions opts)
    {
        #region sample_polecat_skip_errors_like_polecat_5
        opts.Projections.Errors.SkipApplyErrors = true;
        opts.Projections.Errors.SkipSerializationErrors = true;

        // Or leave pc_parked_identities out of the schema too
        opts.Events.EnableParkedIdentities = false;
        #endregion
    }

    public class TripSummary
    {
        public Guid Id { get; set; }
    }
}
