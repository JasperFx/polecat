using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     #652. The exclusive append paths documented a contract they did not keep: a deadlock or a
///     lock timeout was supposed to arrive as <see cref="StreamLockedException" />, and the code read
///     as though SqlClient had already retried it five times. Neither was true. These facts pin what
///     actually happens now, in both directions — the exception the caller gets, and the retry that
///     does not occur.
/// </summary>
[Collection("integration")]
public class exclusive_append_lock_failure_tests
{
    private static DocumentStore CreateStore(string schema)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });
    }

    /// <summary>
    ///     Two sessions take stream locks in opposite order, so SQL Server picks one as the deadlock
    ///     victim and raises 1205 on it. This is the cheapest way to get a REAL 1205 on the append
    ///     path — no lock timeout to configure, and no waiting.
    /// </summary>
    private static async Task<(Exception? Failure, TimeSpan Elapsed)> ForceADeadlock(
        DocumentStore store,
        Func<IDocumentSession, Guid, CancellationToken, Task> takeLock,
        CancellationToken token)
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        await using (var seed = store.LightweightSession())
        {
            seed.Events.StartStream(a, new QuestStarted("A"));
            seed.Events.StartStream(b, new QuestStarted("B"));
            await seed.SaveChangesAsync(token);
        }

        await using var one = store.LightweightSession();
        await using var two = store.LightweightSession();

        await takeLock(one, a, token);
        await takeLock(two, b, token);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var crossOne = Task.Run(async () =>
        {
            try
            {
                await takeLock(one, b, token);
                return (Exception?)null;
            }
            catch (Exception e)
            {
                return e;
            }
        }, token);

        var crossTwo = Task.Run(async () =>
        {
            try
            {
                await takeLock(two, a, token);
                return (Exception?)null;
            }
            catch (Exception e)
            {
                return e;
            }
        }, token);

        var failures = await Task.WhenAll(crossOne, crossTwo);
        stopwatch.Stop();

        // Exactly one of the two is chosen as the victim; the other simply proceeds.
        return (failures.SingleOrDefault(x => x != null), stopwatch.Elapsed);
    }

    [Fact]
    public async Task a_deadlock_during_append_exclusive_surfaces_as_a_stream_locked_exception()
    {
        using var store = CreateStore("lockfail_append");

        var (failure, _) = await ForceADeadlock(
            store,
            (session, id, t) => session.Events.AppendExclusive(id, t, new MonsterSlain("Troll", 2)),
            TestContext.Current.CancellationToken);

        // Before #652 this was a bare SqlException: IsLockFailure only looked at e.InnerException,
        // and the append path throws the SqlException itself, unwrapped.
        var locked = failure.ShouldBeOfType<StreamLockedException>();

        // The SqlException is preserved so a caller can still read the error number.
        locked.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(1205);
    }

    [Fact]
    public async Task a_deadlock_during_fetch_for_exclusive_writing_surfaces_as_a_stream_locked_exception()
    {
        using var store = CreateStore("lockfail_fetch");

        // FetchForExclusiveWriting takes the same UPDLOCK/HOLDLOCK on pc_streams, and had no
        // lock-failure guard whatsoever before #652.
        var (failure, _) = await ForceADeadlock(
            store,
            async (session, id, t) =>
            {
                var stream = await session.Events.FetchForExclusiveWriting<QuestParty>(id, t);
                stream.AppendOne(new MonsterSlain("Troll", 2));
            },
            TestContext.Current.CancellationToken);

        var locked = failure.ShouldBeOfType<StreamLockedException>();
        locked.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(1205);
    }

    /// <summary>
    ///     The attempt count the issue asked about. A 1205 is not retried, and the elapsed time says
    ///     so: the configured provider is five tries with a 1s delta, so a retried deadlock could not
    ///     come back in anything under ~15 seconds — and every retry would re-deadlock anyway, since
    ///     the victim's transaction is already rolled back. The bound is deliberately loose; the
    ///     signal is an order of magnitude, not a millisecond.
    /// </summary>
    [Fact]
    public async Task a_deadlock_is_not_retried_before_the_caller_sees_it()
    {
        using var store = CreateStore("lockfail_noretry");

        var (failure, elapsed) = await ForceADeadlock(
            store,
            (session, id, t) => session.Events.AppendExclusive(id, t, new MonsterSlain("Troll", 2)),
            TestContext.Current.CancellationToken);

        failure.ShouldBeOfType<StreamLockedException>();
        elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(12));
    }

    /// <summary>
    ///     The other half of the analysis, and the reason the timing above holds: the retry provider
    ///     is attached to the CONNECTION, which in Microsoft.Data.SqlClient governs only
    ///     Open()/OpenAsync(). Command execution is retried only via SqlCommand.RetryLogicProvider,
    ///     and Polecat sets that nowhere — so a command carries SqlClient's default provider, which
    ///     is one try. Note the assertion is on the try COUNT, not on null: every SqlCommand hands
    ///     back a non-null "none" provider, so `ShouldBeNull` would read as a refutation of exactly
    ///     the thing being pinned here.
    /// </summary>
    [Fact]
    public void the_retry_provider_governs_the_open_and_not_command_execution()
    {
        var factory = new ConnectionFactory(ConnectionSource.ConnectionString);

        using var connection = factory.Create();

        connection.RetryLogicProvider.RetryLogic.NumberOfTries.ShouldBe(5);

        // Assigning Connection does not hand the command the connection's provider.
        var command = new SqlCommand { Connection = connection };
        command.RetryLogicProvider.RetryLogic.NumberOfTries.ShouldBe(1);
        connection.CreateCommand().RetryLogicProvider.RetryLogic.NumberOfTries.ShouldBe(1);
    }

    /// <summary>
    ///     1205 was in the connection-open transient list, where it could never fire — you cannot be
    ///     chosen as a deadlock victim while opening a connection. Keeping it there implied a retry
    ///     policy Polecat does not have.
    /// </summary>
    [Fact]
    public void the_connection_open_transient_list_does_not_claim_to_retry_deadlocks()
    {
        // SqlClient's own baseline carries 1205 and 1222 too, so dropping the local entry alone
        // would have left the claim standing.
        ConnectionFactory.TransientSqlErrors.ShouldNotContain(1205);
        ConnectionFactory.TransientSqlErrors.ShouldNotContain(1222);

        // 1204 is lock-memory exhaustion rather than contention, and the entries that do real work
        // for the open are all still there.
        ConnectionFactory.TransientSqlErrors.ShouldContain(1204);
        ConnectionFactory.TransientSqlErrors.ShouldContain(-2);
        ConnectionFactory.TransientSqlErrors.ShouldContain(40613);
    }
}
