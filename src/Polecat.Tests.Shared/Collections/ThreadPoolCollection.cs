namespace Polecat.Tests.Harness;

/// <summary>
///     Runs tests that saturate the thread pool on their own. Such a test blocks every pool thread in the process on
///     purpose, which stalls any test running beside it, and work those tests queue would sit in the pool's queue
///     with the test's own.
/// </summary>
[CollectionDefinition("thread-pool", DisableParallelization = true)]
public class ThreadPoolCollection;
