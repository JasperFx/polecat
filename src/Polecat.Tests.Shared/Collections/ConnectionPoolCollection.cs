namespace Polecat.Tests.Harness;

/// <summary>
///     Runs tests that need a pooled connection to survive between two sessions on their own. Several tests
///     call <c>SqlConnection.ClearAllPools()</c>, which empties every pool in the process, so one of them running
///     between the two sessions discarded the connection under test and the second session got a new one.
/// </summary>
[CollectionDefinition("connection-pool", DisableParallelization = true)]
public class ConnectionPoolCollection;
