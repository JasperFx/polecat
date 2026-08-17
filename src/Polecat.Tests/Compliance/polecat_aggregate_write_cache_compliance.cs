using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * #478 / jasperfx#674 -- the second-level aggregate snapshot cache behind FetchForWriting.
 *
 * Opt-in, and the opt-in is the point: a store that ignored it entirely would pass every
 * correctness fact in this suite vacuously, because an uncached fetch is correct by construction.
 * That is what the suite's RecordingAggregateWriteCache and its nonzero-hit assertion exist for --
 * the_cache_is_actually_consulted_when_a_type_opts_in is the fact that separates "implemented" from
 * "silently ignored", and it is the one to look at first if this file ever goes red.
 *
 * The suite deliberately never starts the daemon, so the Async-snapshotted aggregate's stored
 * snapshot always lags and every fetch has a real delta to fold onto whatever baseline it started
 * from. Nothing here can pass by accident of the snapshot already being at the head of the stream.
 */
public class aggregate_write_cache_compliance
    : AggregateWriteCacheCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
