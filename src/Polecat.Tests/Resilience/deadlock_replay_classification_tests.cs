using JasperFx.Events;
using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Resilience;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polly;
using Polly.Retry;

namespace Polecat.Tests.Resilience;

/// <summary>
///     #854. A 1205 means different things by where it landed: outside a transaction, a read that can
///     simply run again; inside one, a transaction that no longer exists. The number cannot tell those
///     apart, so <see cref="PolecatRetryPredicates.IsUnsafeToReplay(Exception, IsolationLevel)" /> answered
///     "safe" for the second one, the strategy spent an attempt, and the caller got the replay's refusal
///     instead of the <see cref="StreamLockedException" /> the lock failure is.
/// </summary>
public class deadlock_replay_classification_tests : OneOffConfigurationsContext
{
    private const int DeadlockVictim = 1205;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private int _retries;
    private readonly List<Exception> _handed = [];

    private void AskThePredicate(IsolationLevel level) => ConfigureStore(opts => opts.ExtendPolly(builder =>
        builder.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = 2,
            Delay = TimeSpan.FromMilliseconds(1),
            ShouldHandle = args =>
            {
                if (args.Outcome.Exception is not { } failure) return ValueTask.FromResult(false);

                lock (_handed) _handed.Add(failure);
                return ValueTask.FromResult(!PolecatRetryPredicates.IsUnsafeToReplay(failure, level));
            },
            OnRetry = _ =>
            {
                Interlocked.Increment(ref _retries);
                return ValueTask.CompletedTask;
            }
        })));

    /// <summary>
    ///     Two sessions lock two streams in opposite order, so SQL Server makes one the victim.
    /// </summary>
    private async Task<Exception> LoseADeadlockAsync()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        await using (var seed = theStore.LightweightSession())
        {
            seed.Events.StartStream(a, new QuestStarted("A"));
            seed.Events.StartStream(b, new QuestStarted("B"));
            await seed.SaveChangesAsync(Ct);
        }

        await using var one = theStore.LightweightSession();
        await using var two = theStore.LightweightSession();

        await one.Events.AppendExclusive(a, Ct, new MonsterSlain("Troll", 2));
        await two.Events.AppendExclusive(b, Ct, new MonsterSlain("Troll", 2));

        async Task<Exception?> Cross(IDocumentSession session, Guid id)
        {
            try
            {
                await session.Events.AppendExclusive(id, Ct, new MonsterSlain("Rat", 1));
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        }

        var failures = await Task.WhenAll(Task.Run(() => Cross(one, b), Ct), Task.Run(() => Cross(two, a), Ct));
        return failures.Single(x => x != null)!;
    }

    [Theory]
    [InlineData(IsolationLevel.ReadCommitted)]
    [InlineData(IsolationLevel.Snapshot)]
    public async Task a_strategy_that_asks_does_not_spend_an_attempt_on_a_transaction_that_is_gone(
        IsolationLevel level)
    {
        AskThePredicate(level);

        var failure = await LoseADeadlockAsync();

        _retries.ShouldBe(0);

        // So the caller gets what actually happened, not the refusal of a replay nobody needed.
        failure.ShouldBeOfType<StreamLockedException>()
            .InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(DeadlockVictim);
    }

    /// <summary>
    ///     <see cref="StreamLockedException" /> is thrown above the pipeline, so what a strategy is handed
    ///     is the bare <see cref="SqlException" />. The answer has to be on that.
    /// </summary>
    [Fact]
    public async Task what_the_strategy_is_handed_already_carries_the_answer()
    {
        AskThePredicate(IsolationLevel.ReadCommitted);

        await LoseADeadlockAsync();

        var handed = _handed.ShouldHaveSingleItem().ShouldBeOfType<SqlException>();
        handed.Number.ShouldBe(DeadlockVictim);
        PolecatRetryPredicates.IsUnsafeToReplay(handed).ShouldBeTrue();
    }
}
