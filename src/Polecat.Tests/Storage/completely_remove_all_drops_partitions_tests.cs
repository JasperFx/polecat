using JasperFx;
using Weasel.Core.Partitioning;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     Distinct type so its database-scoped partition function and scheme cannot collide with any
///     other partitioning test's. See the class remarks for why that matters more than usual here.
/// </summary>
public class RemoveAllSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
}

/// <summary>
///     #718: <c>CompletelyRemoveAllAsync</c> drops the partition scheme and function, not only the
///     tables.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>Nothing in the product dropped these before.</b> The method swept
///         <c>INFORMATION_SCHEMA.TABLES</c> for <c>pc[_]%</c>; a partition function and scheme are
///         not tables, so they were invisible to it. The tell was that <b>six test files hand-rolled
///         the teardown the product did not do</b> — each having discovered it independently.
///     </para>
///     <para>
///         <b>Why a leftover is worse than an ordinary orphan, and why this fact asserts a
///         re-provision rather than only a count.</b> A partition function and scheme are
///         <b>database</b>-scoped, not schema-scoped. So they outlive the schema the call was made
///         for and are shared by every schema in the database — which is exactly how this suite
///         isolates itself. The damage is not the leftover object; it is that a later provision meets
///         a surviving function carrying the OLD boundary set instead of building a fresh one. The
///         same two-disagreeing-descriptions shape as #684, reached from the other end.
///     </para>
/// </remarks>
public class completely_remove_all_drops_partitions_tests: OneOffConfigurationsContext
{
    private const string PartitionFunction = "pf_pc_doc_removeallsample_bucket_end";
    private const string PartitionScheme = "ps_pc_doc_removeallsample_bucket_end";

    private void ConfigurePartitionedStore()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<RemoveAllSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1);
        });
    }

    private async Task<int> CountAsync(string view, string name)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {view} WHERE name = @name";
        cmd.Parameters.AddWithValue("@name", name);
        return (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private Task<int> SchemeCountAsync() => CountAsync("sys.partition_schemes", PartitionScheme);

    private Task<int> FunctionCountAsync() => CountAsync("sys.partition_functions", PartitionFunction);

    [Fact]
    public async Task the_partition_scheme_and_function_are_dropped_with_the_tables()
    {
        var token = TestContext.Current.CancellationToken;

        ConfigurePartitionedStore();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // The precondition. Without it a green assertion below would only mean the objects were
        // never created -- which is how a drop that does nothing looks exactly like one that works.
        (await SchemeCountAsync()).ShouldBe(1, $"'{PartitionScheme}' was never created");
        (await FunctionCountAsync()).ShouldBe(1, $"'{PartitionFunction}' was never created");

        await theStore.Advanced.CompletelyRemoveAllAsync(token);

        (await SchemeCountAsync()).ShouldBe(0, $"'{PartitionScheme}' survived CompletelyRemoveAllAsync");
        (await FunctionCountAsync()).ShouldBe(0, $"'{PartitionFunction}' survived CompletelyRemoveAllAsync");
    }

    [Fact]
    public async Task a_re_provision_afterwards_builds_a_fresh_function()
    {
        // ⚠️ A GUARD rather than a repro, and the difference is worth being honest about: this one
        // passes with the fix disabled. Re-provisioning the SAME configuration meets a surviving
        // function with the same boundaries, so it converges either way. It would only catch a stale
        // function if the configuration changed between the two provisions -- which is the shape the
        // leak actually costs, and is not what this asserts. What it does assert is that the new
        // modeled drop has not broken re-provisioning, which is the regression a drop pass can cause.
        //
        // The fact that genuinely demonstrates the leak is an_unpartitioned_store_is_unaffected
        // below: with the fix disabled it FAILS, because a partition function is database-scoped and
        // another test's leftovers are still sitting there.
        var token = TestContext.Current.CancellationToken;

        ConfigurePartitionedStore();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Advanced.CompletelyRemoveAllAsync(token);

        ConfigurePartitionedStore();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Re-created, exactly once -- not two functions, and not a stale one left in place.
        (await SchemeCountAsync()).ShouldBe(1);
        (await FunctionCountAsync()).ShouldBe(1);

        // And the migration converges, which is what says the surviving-or-rebuilt function agrees
        // with the model rather than merely existing.
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task removing_everything_twice_is_safe()
    {
        // Each strategy's WriteDropDdl is IF EXISTS-guarded, which is what makes the modeled pass
        // safe to run against a database that has already been cleared -- and against one that was
        // never partitioned at all.
        var token = TestContext.Current.CancellationToken;

        ConfigurePartitionedStore();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        await theStore.Advanced.CompletelyRemoveAllAsync(token);
        await theStore.Advanced.CompletelyRemoveAllAsync(token);

        (await SchemeCountAsync()).ShouldBe(0);
        (await FunctionCountAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task an_unpartitioned_store_is_unaffected()
    {
        // The modeled pass emits nothing when no table declares partitioning, so a store that never
        // partitioned anything runs exactly the sweep it always did.
        //
        // ⚠️ And this is the fact that actually demonstrates the bug, which was a surprise: with the
        // fix disabled it FAILS. A partition function and scheme are DATABASE-scoped, so the objects
        // another test in this class created survive into a store that declares no partitioning at
        // all. That is the leak in one assertion -- not an orphan sitting harmlessly in a catalog,
        // but state crossing between two stores that share nothing in their configuration.
        var token = TestContext.Current.CancellationToken;

        ConfigureStore(opts => opts.Schema.For<RemoveAllSample>());
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        await theStore.Advanced.CompletelyRemoveAllAsync(token);

        (await SchemeCountAsync()).ShouldBe(0);
        (await FunctionCountAsync()).ShouldBe(0);
    }
}
