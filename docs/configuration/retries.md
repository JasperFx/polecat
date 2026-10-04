# Resiliency Policies

Polecat executes every database command through a [Polly](https://github.com/App-vNext/Polly)
resilience pipeline on `StoreOptions.ResiliencePipeline`, which you can replace or extend.

## Default behaviour: nothing is retried

::: warning
**Polecat's default pipeline adds no strategy at all.** It does not retry deadlocks, lock timeouts,
or anything else. This page previously claimed the opposite — that Polecat "retries on transient SQL
Server errors (deadlocks, timeouts, connection failures) with an exponential backoff strategy" — and
that was wrong in the most unhelpful direction, because it implied a retry was already in place and
had been thought through.
:::

Two separate things are easy to conflate here:

| | what retries it | which errors |
|---|---|---|
| **Connection open** | Microsoft.Data.SqlClient's own `SqlConnection.RetryLogicProvider` | SqlClient's transient baseline, **minus 1205 and 1222** |
| **Command execution** | nothing, by default | — |

The connection-open list excludes the deadlock victim (1205) and lock request timeout (1222)
deliberately: by the time either is raised the transaction is already rolled back, so nothing worth
calling a retry is possible at the command level — it would have to be a replay of the whole unit of
work. You get a `StreamLockedException` instead and retry at the application layer, e.g. with a
Wolverine `OnException<StreamLockedException>().RetryWithCooldown(...)` policy.

## Before you add a retry

::: danger A retry here replays computed operations, not the code that computed them
By the time the pipeline sees a failure, your application code has already read, decided, and handed
the session a set of operations. A retry re-issues **those**. It does not re-run the method that
produced them, so it cannot recompute anything against fresh data.

For most failures that is fine. For a **snapshot update conflict it is a silent lost update**, because
such a conflict means precisely that the snapshot those operations were derived from is no longer a
valid basis for them. The replay opens a fresh snapshot in which the conflict no longer exists, and
the second write lands over the first.

This is not hypothetical: it is [marten#5528](https://github.com/JasperFx/marten/issues/5528), where
a `Serializable` session lost a committed write exactly this way.
:::

Both preconditions are reachable in Polecat — `SessionOptions.IsolationLevel` is public and honours
`Snapshot` and `Serializable`, and the hooks below invite the retry — so the rule is:

| error | meaning | safe to replay? |
|---|---|---|
| **3960** | snapshot isolation update conflict | **never** — only the application can resolve it, by re-reading and recomputing |
| **1205** | deadlock victim | yes at `ReadCommitted`; **no** under `Snapshot` / `Serializable`, where it replays stale-snapshot work |
| **1222** | lock request timeout | same fork as 1205 |

`PolecatRetryPredicates` encodes that table so you do not have to carry the numbers:

```cs
using Polecat.Resilience;

opts.ExtendPolly(builder =>
{
    builder.AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 3,
        BackoffType = DelayBackoffType.Exponential,
        Delay = TimeSpan.FromMilliseconds(200),

        // The important line. Without it, a 3960 is replayed and a committed write is lost.
        ShouldHandle = args => ValueTask.FromResult(
            args.Outcome.Exception is not null
            && !PolecatRetryPredicates.IsUnsafeToReplay(args.Outcome.Exception))
    });
});
```

Pass the isolation level your sessions use if it is not `ReadCommitted` — it is a per-session
setting, so the predicate cannot read it for you:

```cs
ShouldHandle = args => ValueTask.FromResult(
    args.Outcome.Exception is not null
    && !PolecatRetryPredicates.IsUnsafeToReplay(
        args.Outcome.Exception, IsolationLevel.Snapshot))
```

Polecat does **not** override a strategy you configured. A store that quietly refused to honour an
explicit policy would be a worse surprise than the one it prevents, so the predicate is yours to
compose.

## Custom Polly configuration

### Replace the default pipeline

```cs
opts.ConfigurePolly(builder =>
{
    builder.AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 5,
        BackoffType = DelayBackoffType.Exponential,
        Delay = TimeSpan.FromMilliseconds(200),
        ShouldHandle = args => ValueTask.FromResult(
            args.Outcome.Exception is not null
            && !PolecatRetryPredicates.IsUnsafeToReplay(args.Outcome.Exception))
    });
});
```

### Extend the default pipeline

The defaults are applied first, then your additions — which, since the defaults are empty, means your
additions are the whole pipeline.

```cs
opts.ExtendPolly(builder =>
{
    builder.AddTimeout(TimeSpan.FromSeconds(30));
});
```

## Circuit breaker

A circuit breaker has none of the replay hazard above — it fails fast rather than re-issuing work:

```cs
opts.ExtendPolly(builder =>
{
    builder.AddCircuitBreaker(new CircuitBreakerStrategyOptions
    {
        FailureRatio = 0.5,
        MinimumThroughput = 10,
        SamplingDuration = TimeSpan.FromSeconds(30),
        BreakDuration = TimeSpan.FromSeconds(15)
    });
});
```
