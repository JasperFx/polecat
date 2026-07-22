using Xunit;

// These are integration tests that spin up an Alba host against the single shared SQL Server
// test database and mutate the same document/aggregate tables (clean-all + seed). Running the
// test classes in parallel cross-contaminates that shared state (e.g. one class's CleanAllDocuments
// wiping another's seeded page). Disable assembly-level parallelization so the classes run serially.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
