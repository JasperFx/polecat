using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 10 -- the suites JasperFx 2.51.0 added.
 *
 * PendingStreamActionsCompliance is jasperfx#673 / #477: the stream actions a session has queued but
 * not yet committed, read through the shared document contract. Opt-in for the same reason as
 * DocumentSessionEventsCompliance in wave 9, whose event types and stream marker it reuses -- there
 * are no pending stream actions without an event store.
 *
 * Its first fact is the one a store still on the contract's throwing default cannot pass: an empty
 * collection for a session with nothing enlisted. The default throws rather than answering empty
 * precisely because empty is indistinguishable from a session with nothing pending, so a store that
 * returned empty instead of implementing the member would pass a suite written the other way round
 * and still drop every consumer's work.
 */

public class polecat_pending_stream_actions_compliance
    : PendingStreamActionsCompliance<PolecatDocumentComplianceFixture>;
