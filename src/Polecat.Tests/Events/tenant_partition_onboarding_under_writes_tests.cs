using JasperFx;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record PartitionOnboardingHappened(int Number);

/// <summary>
///     #681 (the marten#5364 shape) — onboarding a tenant partition at runtime while appends are in
///     flight against the same tables.
/// </summary>
/// <remarks>
///     <para>
///         Adding a managed tenant partition is DDL: SQL Server's <c>ALTER PARTITION FUNCTION … SPLIT
///         RANGE</c> takes a schema-modification lock on every table on the scheme, which is
///         <c>pc_events</c> and <c>pc_streams</c> — the tables an append is writing to. Nothing covered
///         the two happening together, and the failure it would produce is not a wrong answer but a
///         thrown append: a lock timeout, a deadlock victim, or a partition-scheme change mid-statement.
///     </para>
///     <para>
///         Deliberately not a timing assertion, because a test that asserts how long a lock is held is
///         a test that fails on a busy CI runner for no reason. The assertions are that <b>no append
///         threw</b>, that every appended event is readable afterwards, and that the new tenant is
///         usable once its partition exists. Bounded iterations rather than a duration, for the same
///         reason.
///     </para>
/// </remarks>
[Collection("tenant-partitioning")]
public class tenant_partition_onboarding_under_writes_tests : IAsyncLifetime
{
    private const string Schema = "pt_onboarding";
    private const string Existing = "incumbent";
    private const string Arriving = "newcomer";
    /// <summary>A runaway guard, not a target: the loop stops when the DDL finishes.</summary>
    private const int MaxAppends = 5000;

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
        => DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
        });

    [Fact]
    public async Task adding_a_tenant_partition_while_appends_are_in_flight_loses_nothing()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        // The incumbent tenant is onboarded and writing before the newcomer arrives, so the SPLIT
        // lands on tables that already hold rows rather than on an empty scheme.
        await store.Advanced.AddPolecatManagedTenantsAsync(Token, Existing);

        var writes = new AppendLoop(store);
        await writes.StartAsync();

        await store.Advanced.AddPolecatManagedTenantsAsync(Token, Arriving);
        var streamIds = await writes.StopAsync();

        // The load-bearing assertion. A schema-modification lock taken while appends are running shows
        // up here as a lock timeout or a deadlock victim, not as a wrong count.
        writes.Failures.ShouldBeEmpty();

        await using (var query = store.QuerySession(new SessionOptions { TenantId = Existing }))
        {
            foreach (var streamId in streamIds)
            {
                (await query.Events.FetchStreamAsync(streamId, token: Token)).Count.ShouldBe(1);
            }
        }

        // And the newcomer's partition is real: it can write, and its rows stay its own.
        var newcomerStream = Guid.NewGuid();
        await using (var newcomer = store.LightweightSession(new SessionOptions { TenantId = Arriving }))
        {
            newcomer.Events.StartStream(newcomerStream, new PartitionOnboardingHappened(99));
            await newcomer.SaveChangesAsync(Token);
        }

        await using var incumbentQuery = store.QuerySession(new SessionOptions { TenantId = Existing });
        (await incumbentQuery.Events.FetchStreamAsync(newcomerStream, token: Token)).ShouldBeEmpty();
    }

    [Fact]
    public async Task removing_a_tenant_partition_while_another_tenant_writes_loses_nothing()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await store.Advanced.AddPolecatManagedTenantsAsync(Token, Existing, Arriving);

        // Give the departing tenant a row, so the MERGE has something to move rather than being a
        // no-op that could not disturb anything.
        await using (var departing = store.LightweightSession(new SessionOptions { TenantId = Arriving }))
        {
            departing.Events.StartStream(Guid.NewGuid(), new PartitionOnboardingHappened(0));
            await departing.SaveChangesAsync(Token);
        }

        var writes = new AppendLoop(store);
        await writes.StartAsync();

        // RetainData: the departing tenant's rows merge into the neighbouring partition. A TRUNCATE
        // before MERGE, which the data-removing behaviour uses, is a different lock again — retaining
        // is the one that has to coexist with live writes.
        await store.Advanced.RemovePolecatManagedTenantsAsync([Arriving], Token);
        var streamIds = await writes.StopAsync();

        writes.Failures.ShouldBeEmpty();

        await using var query = store.QuerySession(new SessionOptions { TenantId = Existing });
        foreach (var streamId in streamIds)
        {
            (await query.Events.FetchStreamAsync(streamId, token: Token)).Count.ShouldBe(1);
        }
    }

    /// <summary>
    ///     Appends to the incumbent tenant continuously until told to stop, so the DDL under test is
    ///     guaranteed to overlap live writes.
    /// </summary>
    /// <remarks>
    ///     A fixed append count was the first shape of this and it was <b>vacuous</b>: 25 sequential
    ///     appends take tens of milliseconds and the SPLIT takes far longer, so the writer finished
    ///     before the DDL began and the test proved nothing. <see cref="StopAsync" /> therefore asserts
    ///     that appends actually landed <em>while</em> the DDL was running — the precondition, checked
    ///     rather than assumed, because without it every assertion below still passes for a reason with
    ///     nothing to do with this issue.
    /// </remarks>
    private sealed class AppendLoop(DocumentStore store)
    {
        private readonly List<Guid> _streamIds = [];
        private readonly List<Exception> _failures = [];
        private readonly CancellationTokenSource _stop = new();
        private readonly TaskCompletionSource _firstAppend =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private Task? _loop;
        private int _countWhenDdlStarted = -1;

        public IReadOnlyList<Exception> Failures
        {
            get { lock (_failures) return _failures.ToList(); }
        }

        /// <summary>Begin appending, and return once at least one append has committed.</summary>
        public async Task StartAsync()
        {
            _loop = Task.Run(AppendUntilStoppedAsync);
            await _firstAppend.Task;
            lock (_streamIds) _countWhenDdlStarted = _streamIds.Count;
        }

        public async Task<IReadOnlyList<Guid>> StopAsync()
        {
            await _stop.CancelAsync();
            if (_loop != null) await _loop;

            List<Guid> ids;
            lock (_streamIds) ids = _streamIds.ToList();

            ids.Count.ShouldBeGreaterThan(_countWhenDdlStarted,
                "no append committed while the partition DDL was running, so this test would pass "
                + "without ever exercising the concurrency it exists to cover");

            return ids;
        }

        private async Task AppendUntilStoppedAsync()
        {
            var i = 0;
            while (!_stop.IsCancellationRequested && i < MaxAppends)
            {
                try
                {
                    await using var session = store.LightweightSession(
                        new SessionOptions { TenantId = Existing });
                    var streamId = Guid.NewGuid();
                    session.Events.StartStream(streamId, new PartitionOnboardingHappened(i));
                    await session.SaveChangesAsync(CancellationToken.None);

                    lock (_streamIds) _streamIds.Add(streamId);
                    _firstAppend.TrySetResult();
                }
                catch (Exception e)
                {
                    lock (_failures) _failures.Add(e);
                    // Unblock the caller either way: a first append that FAILED is itself the finding,
                    // and hanging here would report it as a timeout instead.
                    _firstAppend.TrySetResult();
                }

                i++;
            }

            _firstAppend.TrySetResult();
        }
    }
}
