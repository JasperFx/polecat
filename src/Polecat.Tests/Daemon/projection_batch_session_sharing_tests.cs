using Polecat.Events.Daemon;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Weasel.Storage;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Session-semantics audit (Stoat plan critter-hardening-audit, node
///     polecat-session-semantics-audit) — the Polecat analogue of marten#4657 / #4667.
///
///     Marten's async daemon handed concurrent projection slices sessions that shared mutable
///     session state — the <c>VersionTracker</c>, the <c>ItemMap</c>, and the <c>ChangeTrackers</c>
///     list. Two slices projecting at once raced on those structures: torn dictionary reads, a
///     version recorded by one slice asserted by another, and user-code <c>LoadAsync</c> inside a
///     projection populating a map another slice was reading. Marten's fix routed the projection
///     read and write paths around the session-shared trackers entirely (the <c>*Projected</c>
///     variants and <c>LoadProjectedAsync</c>).
///
///     Polecat's <see cref="PolecatProjectionBatch"/> holds a <c>ConcurrentBag&lt;IDocumentSession&gt;</c>,
///     which raises the same question: what do those sessions share? These tests pin the answer —
///     every <c>SessionForTenant</c> call constructs a <em>fresh</em> session, so there is no
///     session-shared tracker for concurrent slices to race on in the first place. That is a
///     structural property worth holding in place: a future change that memoized sessions per tenant
///     (an obvious-looking optimization, since the batch may call this many times for one tenant)
///     would reintroduce exactly Marten's bug.
/// </summary>
public class projection_batch_session_sharing_tests : OneOffConfigurationsContext
{
    /// <summary>
    ///     Concurrent slices must not be handed the same session object, and must not be handed
    ///     sessions that alias each other's identity map, version tracker, or change-tracker list.
    ///     Every one of those three is a structure marten#4657/#4667 raced on.
    /// </summary>
    [Fact]
    public async Task concurrent_slices_get_sessions_with_no_shared_mutable_state()
    {
        ConfigureStore(opts => opts.DatabaseSchemaName = "batch_session_sharing");
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(
            ct: TestContext.Current.CancellationToken);

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        await using var _ = batch;

        var tenant = theStore.Options.Tenancy!.DefaultTenantId;
        const int slices = 16;

        // All slices ask for a session at once, the way composite projections do.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = Enumerable.Range(0, slices).Select(_ => Task.Run(async () =>
        {
            await gate.Task;
            return batch.SessionForTenant(tenant);
        })).ToArray();

        gate.SetResult();
        var sessions = await Task.WhenAll(tasks);

        sessions.Length.ShouldBe(slices);
        sessions.ShouldAllBe(s => s != null);

        // Distinct session objects...
        sessions.Distinct(ReferenceEqualityComparer.Instance).Count().ShouldBe(slices);

        // ...and, more to the point, distinct mutable state. Reference-sharing any of these three
        // is what made Marten's slices race.
        var storage = sessions.Cast<IStorageSession>().ToArray();

        storage.Select(s => s.ItemMap).Distinct(ReferenceEqualityComparer.Instance)
            .Count().ShouldBe(slices, "concurrent slices must not share an ItemMap");

        storage.Select(s => s.Versions).Distinct(ReferenceEqualityComparer.Instance)
            .Count().ShouldBe(slices, "concurrent slices must not share a VersionTracker");

        storage.Select(s => s.ChangeTrackers).Distinct(ReferenceEqualityComparer.Instance)
            .Count().ShouldBe(slices, "concurrent slices must not share a ChangeTrackers list");

        // Each session also needs its own unit of work, or two slices' operations would interleave
        // into one tracker and the batch would flush them twice.
        storage.Cast<DocumentSessionBase>().Select(s => s.WorkTracker)
            .Distinct(ReferenceEqualityComparer.Instance)
            .Count().ShouldBe(slices, "concurrent slices must not share a WorkTracker");
    }

    /// <summary>
    ///     The batch must still collect every one of those sessions' work — isolation is only
    ///     correct if the operations all land in the one transaction the batch commits. This is the
    ///     other half of the invariant: isolated state, shared flush.
    /// </summary>
    [Fact]
    public async Task work_from_every_concurrent_slice_lands_in_the_batch()
    {
        ConfigureStore(opts => opts.DatabaseSchemaName = "batch_session_flush");
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(
            ct: TestContext.Current.CancellationToken);

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);

        var tenant = theStore.Options.Tenancy!.DefaultTenantId;
        const int slices = 12;
        var ids = Enumerable.Range(0, slices).Select(_ => Guid.NewGuid()).ToArray();

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = ids.Select(id => Task.Run(async () =>
        {
            await gate.Task;
            var session = batch.SessionForTenant(tenant);
            session.Store(new SlicedDoc { Id = id });
        })).ToArray();

        gate.SetResult();
        await Task.WhenAll(tasks);

        await batch.ExecuteAsync(TestContext.Current.CancellationToken);
        await batch.DisposeAsync();

        await using var query = theStore.QuerySession();
        var stored = await query.LoadManyAsync<SlicedDoc>(ids, TestContext.Current.CancellationToken);

        stored.Count.ShouldBe(slices);
        stored.Select(x => x.Id).OrderBy(x => x)
            .ShouldBe(ids.OrderBy(x => x));
    }

    /// <summary>
    ///     A projection that reads before it writes is where marten#4667 actually bit: user-code
    ///     LoadAsync inside a projection populated session-shared state another slice was using.
    ///     Concurrent read-then-write slices must each see their own document and produce their own
    ///     write, with no cross-contamination.
    /// </summary>
    [Fact]
    public async Task concurrent_slices_that_read_before_writing_do_not_contaminate_each_other()
    {
        ConfigureStore(opts => opts.DatabaseSchemaName = "batch_session_read_write");
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(
            ct: TestContext.Current.CancellationToken);

        const int slices = 12;
        var ids = Enumerable.Range(0, slices).Select(_ => Guid.NewGuid()).ToArray();

        // Seed one document per slice.
        await using (var seed = theStore.LightweightSession())
        {
            foreach (var id in ids)
            {
                seed.Store(new SlicedDoc { Id = id });
            }

            await seed.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        var tenant = theStore.Options.Tenancy!.DefaultTenantId;

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loaded = new System.Collections.Concurrent.ConcurrentDictionary<Guid, Guid>();

        var tasks = ids.Select(id => Task.Run(async () =>
        {
            await gate.Task;
            var session = batch.SessionForTenant(tenant);

            // The read marten#4667 routed around: user code loading through the projection session.
            var doc = await session.LoadAsync<SlicedDoc>(id, TestContext.Current.CancellationToken);
            loaded[id] = doc?.Id ?? Guid.Empty;

            session.Store(new SlicedDoc { Id = id });
        })).ToArray();

        gate.SetResult();
        await Task.WhenAll(tasks);

        await batch.ExecuteAsync(TestContext.Current.CancellationToken);
        await batch.DisposeAsync();

        // Every slice must have read back its OWN document, not a neighbour's and not null.
        foreach (var id in ids)
        {
            loaded[id].ShouldBe(id, $"slice {id} read back the wrong document");
        }
    }

    /// <summary>Document type owned by this audit so it does not share a table with other suites.</summary>
    public class SlicedDoc
    {
        public Guid Id { get; set; }
    }
}
