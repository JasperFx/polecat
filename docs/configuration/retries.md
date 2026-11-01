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
deliberately: neither can happen while a connection opens. Nor are they the same failure once one is
open. A deadlock rolls the transaction back, so nothing worth calling a retry is possible at the
command level — it would have to be a replay of the whole unit of work. A lock timeout aborts the
statement and leaves the transaction open. Either way, on the exclusive append paths you get a
`StreamLockedException` and retry at the application layer, e.g. with a Wolverine
`OnException<StreamLockedException>().RetryWithCooldown(...)` policy. If a strategy on this pipeline
retries the deadlock first, you get `SessionTransactionUnusableException` instead, because the replay
is refused, so handle that too or leave 1205 out of the strategy.

## Before you add a retry

::: danger A retry here replays one command, not the code that computed it
The pipeline wraps each command, not the unit of work. By the time it sees a failure, your application
code has already read, decided, and handed the session a set of operations. A retry re-issues **the
command that failed**. It does not re-run the method that produced it, so it cannot recompute anything
against fresh data, and it does not re-run the commands before it in the same transaction.

For most failures that is fine. For a **snapshot update conflict it is a silent lost update**, because
such a conflict means precisely that the snapshot those operations were derived from is no longer a
valid basis for them. The replay opens a fresh snapshot in which the conflict no longer exists, and
the second write lands over the first.

This is not hypothetical: it is [marten#5528](https://github.com/JasperFx/marten/issues/5528), where
a `Serializable` session lost a committed write exactly this way.
:::

Both preconditions are reachable in Polecat — `SessionOptions.IsolationLevel` is public and sets the
level of every transaction a document session runs ([see below](#what-the-isolation-level-covers)), and
the hooks below invite the retry — so the rule is:

| error | meaning | what happens to the transaction | safe to replay? |
|---|---|---|---|
| **3960** | snapshot isolation update conflict | rolled back | **never** — only the application can resolve it, by re-reading and recomputing |
| **1205** | deadlock victim | rolled back | **never inside a transaction**, at any level: the work before this command is gone, and replaying the command cannot bring it back. Outside one (a plain read), yes at `ReadCommitted`; **no** under `Snapshot` / `Serializable` |
| **1222** | lock request timeout | **stays open**, only the statement is aborted | yes at `ReadCommitted`; **no** under `Snapshot` / `Serializable`, where it replays stale-snapshot work |

`PolecatRetryPredicates` encodes that table so you do not have to carry the numbers. An error number
cannot tell the two rows of 1205 apart, so at `ReadCommitted` the predicate answers "safe" for a
deadlock and the protection sits below it, as described next. That costs one attempt: the replay is
refused, the predicate answers "unsafe" for the refusal, and the retry stops there.

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

The one thing it will not do is run a command outside a transaction the session believes it is inside.
A strategy that retries a command whose transaction has gone, whether or not it asked the predicate,
has the replay refused with `SessionTransactionUnusableException`, which carries
the original failure as its `InnerException`. Without that refusal the replay would autocommit: every
later statement in the save would be durable, and the commit would then report that the save failed.

## When SQL Server ends the transaction

A deadlock (1205), a snapshot update conflict (3960) and a handful of rarer errors roll the transaction
back on the server while the connection stays open. What happens next depends on what the session was
relying on that transaction for:

| the transaction was... | what is gone with it | what the session does |
|---|---|---|
| holding an exclusive lock (`FetchForExclusiveWriting`, `AppendExclusive`, natural-key exclusive fetches) | the lock the caller made decisions under | **finished** |
| any transaction of a session at another isolation level, such as `Snapshot` or `Serializable` | the view of the data the caller read | **finished** |
| only the failed save's own, in a `ReadCommitted` session | nothing the caller has seen | keeps working: that save failed atomically, nothing is durable, and the next save begins a clean transaction with the work still pending |

A finished session refuses everything from then on, reads included, with
`SessionTransactionUnusableException`, and writes nothing. Dispose it, open a new session and redo the
work from its inputs. Under Wolverine a message retry does exactly that.

A lock timeout (1222) is not on this list. The statement failed, the transaction and every lock it holds
are still there, and the session carries on.

## What the isolation level covers

`SessionOptions.IsolationLevel` applies to every document session, however it was opened, and to every
transaction that session runs, not just the first. At anything but `ReadCommitted` the session also
runs its reads inside a transaction, beginning one with the first command and the next one with the
first command after each commit, so a read and the save that follows it are one transaction. That
transaction stays open between commands, so keep these sessions short.

A **query session** never opens a transaction, so the option does nothing there: each read takes its own
connection and runs in autocommit at `ReadCommitted`. For several reads against one snapshot, use a
document session at that level.

::: warning Document writes do not raise 3960
`Store` and `Update` are `MERGE ... WITH (HOLDLOCK)`, and SQL Server does not raise a snapshot update
conflict for a statement carrying that hint: it reads the latest committed row and writes over it. So
in a `Snapshot` session those two **overwrite a concurrent write**, silently, even though every
statement really is running at `Snapshot`. Event appends, and SQL you queue yourself without that hint,
do raise 3960. For documents, use [optimistic concurrency](/documents/storage), which compares the
stored version and raises `ConcurrencyException` at any isolation level.
:::

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
