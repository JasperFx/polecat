# Diagnostics and Instrumentation

Polecat provides several tools for monitoring, debugging, and understanding what's happening in your application.

## Session Logging

### IPolecatLogger

Implement `IPolecatLogger` at the store level to create per-session loggers:

```cs
public class ConsolePolecatLogger : IPolecatLogger
{
    public IPolecatSessionLogger StartSession(IQuerySession session)
    {
        return new ConsoleSessionLogger();
    }
}

public class ConsoleSessionLogger : IPolecatSessionLogger
{
    public void OnBeforeExecute(string sql)
    {
        Console.WriteLine($"Executing: {sql}");
    }

    public void LogSuccess(string sql)
    {
        Console.WriteLine($"Success: {sql}");
    }

    public void LogFailure(string sql, Exception ex)
    {
        Console.WriteLine($"Failed: {sql} - {ex.Message}");
    }

    public void RecordSavedChanges(IDocumentSession session)
    {
        Console.WriteLine($"Saved changes ({session.RequestCount} requests)");
    }
}
```

Register the logger:

```cs
opts.Logger(new ConsolePolecatLogger());
```

### Request Counting

Track database requests per session:

```cs
await using var session = store.LightweightSession();
await session.LoadAsync<User>(id);
Console.WriteLine(session.RequestCount); // 1
```

## SQL Preview

### ToSql

Preview the SQL a LINQ query runs, without running it:

```cs
var query = session.Query<User>().Where(x => x.LastName == "Smith");

Console.WriteLine(session.ToSql(query));
// SELECT data FROM pc_doc_user WHERE JSON_VALUE(data, '$.lastName') = @p0
```

The preview is built by the same preparation execution uses, so it is the text that runs.

`ToSql(query)` shows what `ToListAsync()` sends. A terminal operator sends a different statement, and
passing a `FetchType` previews that one instead (the same enum, and the same four members, as Marten's
`ToCommand(FetchType)`):

```cs
session.ToSql(query, FetchType.Count);    // what CountAsync() sends
session.ToSql(query, FetchType.Any);      // AnyAsync()
session.ToSql(query, FetchType.FetchOne); // FirstAsync() / FirstOrDefaultAsync()
session.ToSql(query, FetchType.FetchMany); // ToListAsync(), the same as ToSql(query)
```

For any other terminal -- `LongCount()`, `Single()`, `Last()`, an operator with a predicate, or an
aggregate -- hand the operator itself to `ToSql`. The expression is never compiled or run, so write the
synchronous `Queryable` operator that the async one executes:

```cs
session.ToSql(query, q => q.Sum(x => x.Age));     // what SumAsync(x => x.Age) sends
session.ToSql(query, q => q.LongCount());         // LongCountAsync()
session.ToSql(query, q => q.Single());            // SingleAsync(): reads up to two rows
session.ToSql(query, q => q.Count(x => x.Age > 21));
```

A paged query previews the shape that actually runs: `Take(3)` in front of a count counts a derived table
of the page, not the table. A `GroupJoin` has no single statement, and its preview is refused.

## Schema Diagnostics

### ToDatabaseScript

Generate the complete DDL script for all Polecat tables:

```cs
var script = await store.Advanced.ToDatabaseScript();
Console.WriteLine(script);
```

### WriteCreationScriptToFileAsync

Save the schema script to a file:

```cs
await store.Advanced.WriteCreationScriptToFileAsync("/path/to/schema.sql");
```

### Reads against a schema that is not there

Every diagnostic read of the event-store tables — projection progression, dead-letter counts and rows,
the highest event sequence, the event-store floor at a timestamp, and
`Advanced.FetchEventStoreStatistics()` — answers **"no results"** rather than throwing when the storage
it reads does not exist. Empty, `null` or `0`, depending on the read.

Two situations reach it, and both are ordinary:

* **`AutoCreate.None` against a database where the schema was never applied.** Being in the migration
  set does not help if nothing ever applies it.
* **Column drift.** A `pc_event_progression` that hasn't been migrated since before Polecat 6 can be
  missing the `heartbeat` / `agent_status` / `pause_reason` columns the progression reads select.

This matters most for a monitoring tool polling a fleet: one store in the fan-out throwing would
otherwise abort the whole poll, and the operator gets an empty page for the shards that *do* have data.
The decision is made from the database's own error rather than from configuration, because drift is
invisible to configuration — a store can declare extended tracking, have every table in its migration
set, and still be pointed at a schema where neither is true.

::: warning
This applies to diagnostic **reads** only. Writes still throw: silently dropping a progression or
telemetry write is a worse failure than a read returning nothing. The single exception is
`DeleteProjectionProgressByShardNameAsync`, whose contract already promises a clean no-op for a row that
is not there.
:::

## Data Cleanup

### CleanAllDocumentsAsync

Delete all data from all document tables:

```cs
await store.Advanced.CleanAllDocumentsAsync();
```

### CleanAsync

Delete all data from a specific document table:

```cs
await store.Advanced.CleanAsync<User>();
```

### CleanAllEventDataAsync

Delete all event data (events, streams, progressions):

```cs
await store.Advanced.CleanAllEventDataAsync();
```

::: warning
These cleanup methods permanently delete data. They are intended for testing and development, not production use.
:::

## Projection Daemon Monitoring

### Extended Progression Tracking

The event-progression table (`pc_event_progression`) has extra columns that the asynchronous
projection daemon uses to report per-shard health — the same surface monitoring tools such as
**CritterWatch** read to show heartbeat liveness, agent status, pause reasons, and per-shard alert
thresholds.

This is on by default since Polecat 6, as it is in Marten 10. To turn it off:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Events.EnableExtendedProgressionTracking = false;
});
```

`RestoreV5Defaults()` turns it off too.

The nullable columns (`heartbeat`, `agent_status`, `pause_reason`, `running_on_node`,
`warning_behind_threshold`, `critical_behind_threshold`, the `failure_*` columns and `parked_count`) are
part of `pc_event_progression` whatever the setting says. With it on, the daemon writes its runtime agent
state to them and they are read back into `JasperFx.Events.Projections.ShardState`. With it off, they stay
`NULL`. Keeping the columns either way means two applications sharing a schema with different settings
don't drop each other's columns when they migrate.

A shard that fails before its first commit, like one that hits the parking cap on its first batch, has no row
yet. Its failure creates the row at the position the shard started from, so the parked identities health check
on another node, the CLI and CritterWatch can see it. The row doesn't change where the shard starts next time,
and the shard's first commit takes it over and clears the failure.

`StoreOptions.Events` also implements the storage-agnostic
`JasperFx.Events.IEventStoreInstrumentation` interface, whose `ExtendedProgressionEnabled`
property is an alias for `EnableExtendedProgressionTracking`. This lets store-agnostic tooling
toggle the same monitoring fidelity across Marten and Polecat without referencing store-specific
types:

```cs
IEventStoreInstrumentation instrumentation = store.Options.Events;
instrumentation.ExtendedProgressionEnabled = false;
```
