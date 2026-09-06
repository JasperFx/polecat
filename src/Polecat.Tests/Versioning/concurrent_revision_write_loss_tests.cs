using JasperFx;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Versioning;

/// <summary>
///     Session-semantics audit (Stoat plan critter-hardening-audit, node
///     polecat-session-semantics-audit) — the Polecat analogue of Marten c09eed24c / c8e851722.
///
///     Marten's <c>mt_upsert_&lt;doc&gt;</c> ran a conditional <c>ON CONFLICT … DO UPDATE …
///     WHERE revision &gt; mt_version</c> and then unconditionally re-SELECTed <c>mt_version</c>
///     into its return value. When a concurrent transaction had already moved the version past the
///     caller's revision, the UPDATE matched zero rows but the function still returned the (bumped)
///     version — and Marten reads any non-zero return as success. SaveChangesAsync did not throw,
///     AfterCommitAsync fired, and the write was silently gone. The fix made the returned value come
///     from the UPDATE itself (<c>RETURNING</c>), so a filtered-out update returns nothing.
///
///     Polecat's equivalent is the guarded MERGE built by SqlServerDocumentStorageDescriptorBuilder:
///     <c>WHEN MATCHED AND (? = 0 OR t.version = ?) THEN UPDATE … OUTPUT inserted.version</c>. OUTPUT
///     is MERGE's RETURNING — it only emits a row for a row the MERGE actually acted on — so the
///     structure carries the fix by construction. These tests exercise that claim with genuine
///     concurrency rather than asserting on SQL shape: the invariant under test is that N racing
///     writers produce exactly (number that reported success) surviving writes, and that a write
///     which does not land is reported as a failure rather than silently dropped.
/// </summary>
[Collection("integration")]
public class concurrent_revision_write_loss_tests : IntegrationContext
{
    public concurrent_revision_write_loss_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "rev_write_loss");
    }

    /// <summary>
    ///     Two sessions load the same revision-1 document and both call UpdateRevision(doc, 1).
    ///     Exactly one may win. The loser must throw — not report success over a write that never
    ///     landed. The discriminating assertion is the pairing: the winner's name must be the one in
    ///     the database. A silent write-loss shows up as "both succeeded" with only one name stored.
    /// </summary>
    [Fact]
    public async Task concurrent_update_revision_does_not_silently_lose_writes()
    {
        var id = Guid.NewGuid();

        theSession.Insert(new RevisionedDoc { Id = id, Name = "initial" });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var s1 = theStore.LightweightSession();
        await using var s2 = theStore.LightweightSession();

        var d1 = await s1.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
        var d2 = await s2.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
        d1.ShouldNotBeNull();
        d2.ShouldNotBeNull();
        d1.Version.ShouldBe(1);
        d2.Version.ShouldBe(1);

        d1.Name = "writer-1";
        d2.Name = "writer-2";
        s1.UpdateRevision(d1, 1);
        s2.UpdateRevision(d2, 1);

        // Genuinely concurrent: both flushes are in flight at once.
        var r1 = SaveOutcomeAsync(s1, "writer-1");
        var r2 = SaveOutcomeAsync(s2, "writer-2");
        var outcomes = await Task.WhenAll(r1, r2);

        var winners = outcomes.Where(x => x.Succeeded).ToList();
        var losers = outcomes.Where(x => !x.Succeeded).ToList();

        winners.Count.ShouldBe(1);
        losers.Count.ShouldBe(1);
        losers.Single().Error.ShouldBeOfType<ConcurrencyException>();

        // The surviving row must be the write that reported success. If the loser had been told it
        // succeeded while its UPDATE was filtered out, this is where it shows.
        await using var query = theStore.QuerySession();
        var stored = await query.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
        stored.ShouldNotBeNull();
        stored.Name.ShouldBe(winners.Single().Name);
        stored.Version.ShouldBe(2);
    }

    /// <summary>
    ///     The same invariant at wider concurrency, which is where a lost-update window actually
    ///     shows up. Eight writers race on one document from the same starting revision. However many
    ///     report success, the database must agree with exactly one of them, and no writer may report
    ///     success for a write that is not the stored one.
    /// </summary>
    [Fact]
    public async Task racing_writers_never_report_success_for_a_write_that_did_not_land()
    {
        const int writers = 8;
        var id = Guid.NewGuid();

        theSession.Insert(new RevisionedDoc { Id = id, Name = "initial" });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sessions = new List<IDocumentSession>();
        var tasks = new List<Task<SaveOutcome>>();

        try
        {
            for (var i = 0; i < writers; i++)
            {
                var session = theStore.LightweightSession();
                sessions.Add(session);

                var name = $"writer-{i}";
                var doc = await session.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
                doc.ShouldNotBeNull();
                doc.Name = name;
                session.UpdateRevision(doc, 1);

                tasks.Add(Task.Run(async () =>
                {
                    await gate.Task;
                    return await SaveOutcomeAsync(session, name);
                }));
            }

            gate.SetResult();
            var outcomes = await Task.WhenAll(tasks);

            var winners = outcomes.Where(x => x.Succeeded).ToList();

            // At most one writer can move version 1 -> 2. Any second "success" is a lost update.
            winners.Count.ShouldBe(1);

            await using var query = theStore.QuerySession();
            var stored = await query.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
            stored.ShouldNotBeNull();
            stored.Version.ShouldBe(2);
            stored.Name.ShouldBe(winners.Single().Name);

            foreach (var loser in outcomes.Where(x => !x.Succeeded))
            {
                loser.Error.ShouldBeOfType<ConcurrencyException>();
            }
        }
        finally
        {
            foreach (var session in sessions)
            {
                await session.DisposeAsync();
            }
        }
    }

    /// <summary>
    ///     The auto-increment path (revision 0, no explicit guard) must still not lose a write:
    ///     every writer that reports success must have moved the version, so the final version
    ///     equals 1 + the number of successes.
    /// </summary>
    [Fact]
    public async Task auto_increment_stores_account_for_every_reported_success()
    {
        const int writers = 6;
        var id = Guid.NewGuid();

        theSession.Insert(new RevisionedDoc { Id = id, Name = "initial" });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sessions = new List<IDocumentSession>();
        var tasks = new List<Task<SaveOutcome>>();

        try
        {
            for (var i = 0; i < writers; i++)
            {
                var session = theStore.LightweightSession();
                sessions.Add(session);

                // Revision 0 => the MERGE's auto-increment branch, no version guard.
                var name = $"writer-{i}";
                session.Store(new RevisionedDoc { Id = id, Name = name, Version = 0 });

                tasks.Add(Task.Run(async () =>
                {
                    await gate.Task;
                    return await SaveOutcomeAsync(session, name);
                }));
            }

            gate.SetResult();
            var outcomes = await Task.WhenAll(tasks);

            var successes = outcomes.Count(x => x.Succeeded);
            successes.ShouldBeGreaterThan(0);

            await using var query = theStore.QuerySession();
            var stored = await query.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
            stored.ShouldNotBeNull();

            // Every success must be one version bump. A silently-dropped update shows as a version
            // lower than the number of writes that claimed to succeed.
            stored.Version.ShouldBe(1 + successes);
        }
        finally
        {
            foreach (var session in sessions)
            {
                await session.DisposeAsync();
            }
        }
    }

    /// <summary>
    ///     Flush and report the outcome instead of throwing, so a racing pair can be compared. The
    ///     name is passed in rather than read back off the session: the work tracker is drained by a
    ///     successful flush, so reading it afterwards would report nothing for exactly the writers
    ///     whose result matters most.
    /// </summary>
    private static async Task<SaveOutcome> SaveOutcomeAsync(IDocumentSession session, string name)
    {
        try
        {
            await session.SaveChangesAsync();
            return new SaveOutcome(true, name, null);
        }
        catch (Exception e)
        {
            return new SaveOutcome(false, name, e);
        }
    }

    private sealed record SaveOutcome(bool Succeeded, string Name, Exception? Error);
}
