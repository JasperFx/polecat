namespace Polecat.Tests.Harness;

/// <summary>
///     polecat#1049: serializes the Async fetch race tests. Each of their tests rebuilds an event store and runs
///     the async daemon, which reads the catalog to ensure its schema. While archived-stream partitioning was the
///     6.0 alphas' default, every schema's <c>pc_events</c> sat on the one database-global
///     <c>ps_pc_events_is_archived</c> partition scheme, so that read deadlocked now and then with another class
///     creating or dropping its event store at the same moment. Schemas that turn partitioning on still share it.
/// </summary>
[CollectionDefinition("async-fetch", DisableParallelization = true)]
public class AsyncFetchCollection;
