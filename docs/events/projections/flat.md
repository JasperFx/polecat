# Flat Table Projections

Flat table projections write events directly to SQL Server tables with defined columns, rather than as JSON documents. This is ideal for reporting and analytics scenarios.

## Defining a Flat Table Projection

```cs
public class OrderFlatTableProjection : FlatTableProjection
{
    public OrderFlatTableProjection() : base("order_history")
    {
        // Define table columns
        Table.AddColumn<Guid>("id").AsPrimaryKey();
        Table.AddColumn<Guid>("stream_id");
        Table.AddColumn<string>("status");
        Table.AddColumn<decimal>("amount");
        Table.AddColumn<DateTimeOffset>("event_time");

        // Map events to columns
        Project<OrderCreated>(map =>
        {
            map.Map(e => e.Amount, "amount");
            map.SetValue("status", "Created");
        });

        Project<OrderShipped>(map =>
        {
            map.SetValue("status", "Shipped");
        });

        // Delete rows on certain events
        Delete<OrderCancelled>();
    }
}
```

## Registration

```cs
opts.Projections.Add<OrderFlatTableProjection>(ProjectionLifecycle.Async);
```

## Column Mapping

### Map Event Properties

```cs
map.Map(e => e.Amount, "amount");
map.Map(e => e.CustomerName, "customer_name");
```

### Set Static Values

```cs
map.SetValue("status", "Shipped");
map.SetValue("processed", true);
```

## SQL Generation

Flat table projections use SQL Server `MERGE` statements for upsert behavior, ensuring idempotent processing.

## Schema Management

A flat table is part of the event store's schema, so it follows the same rules as `pc_events`:

- `ApplyAllConfiguredChangesToDatabaseAsync()`, `db-apply` and the async daemon create it, and so does a session's first use of the event store, in every tenant database.
- `AutoCreate.None` never creates it. A store that manages its own schema has to create the table, and a missing table fails the save with an ordinary "Invalid object name" error.
- `Advanced.ToDatabaseScript()` and `db-dump` include it, and `AssertDatabaseMatchesConfigurationAsync()` and `db-assert` report it when it is missing or out of date.

Migrations are additive. A column or index the projection declares is added to an existing table, and one it does not declare is left alone, so a column kept from an old mapping or an index added for a report survives. Dropping one is up to you.

Table, column and index names go through Weasel's identifier rules like every other schema object: `;`, `'`, `"`, `[` and `]` are refused at startup.

### The Table's Schema

A flat table goes in `dbo` unless you pass a schema to the constructor, whatever `DatabaseSchemaName` the store uses:

```cs
public OrderFlatTableProjection() : base("order_summary", "sales")
```

Pass one if you have more than one store in a database. Two stores in different schemas that register the same projection with no schema of their own share one `dbo` table, while each takes the migration lock for its own schema, so neither lock keeps the other out of it.

## Use Cases

- **Reporting tables** -- Denormalized data for BI tools
- **Analytics** -- Pre-computed metrics per event
- **Audit trails** -- Structured event history in relational format
- **Integration** -- Data accessible by non-.NET systems via standard SQL

::: tip
Flat table projections bypass the document storage layer entirely, writing directly to user-defined SQL tables. This makes the data accessible to any SQL tool without understanding JSON document structure.
:::

## Using EventProjection for Flat Tables

::: tip
The `EventProjection` approach shown below is more explicit code than `FlatTableProjection`, but it is also
more flexible. Use `EventProjection` when you need full control over the SQL being generated, need to access
event metadata through the `IEvent<T>` envelope, or when the declarative `FlatTableProjection` API does not
support your use case. The tradeoff is that you are writing raw SQL yourself, so you are responsible for
getting the SQL correct and handling upsert logic on your own.
:::

As an alternative to the more rigid `FlatTableProjection` approach, you can use Polecat's `EventProjection` as a
base class and write explicit SQL to project events into a flat table. This gives you complete control over the
SQL operations and full access to event metadata:

<!-- snippet: sample_polecat_import_sql_event_projection -->
<a id='snippet-sample_polecat_import_sql_event_projection'></a>
```cs
public partial class ImportSqlProjection: EventProjection
{
    // Use the IEvent<T> envelope to access event metadata
    // like stream identity and timestamps
    public void Project(IEvent<ImportStarted> e, IDocumentSession ops)
    {
        ops.QueueSqlCommand(
            "insert into import_history (id, activity_type, customer_id, started) values (?, ?, ?, ?)",
            e.StreamId, e.Data.ActivityType, e.Data.CustomerId, e.Data.Started
        );
    }

    public void Project(IEvent<ImportFinished> e, IDocumentSession ops)
    {
        ops.QueueSqlCommand(
            "update import_history set finished = ? where id = ?",
            e.Data.Finished, e.StreamId
        );
    }

    // You can use any SQL operation, including deletes
    public void Project(IEvent<ImportFailed> e, IDocumentSession ops)
    {
        ops.QueueSqlCommand(
            "delete from import_history where id = ?",
            e.StreamId
        );
    }
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Projections/using_event_projection_for_flat_tables.cs#L25-L57' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_import_sql_event_projection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A couple notes about the `EventProjection` approach:

* **Batched execution** -- The `QueueSqlCommand()` method doesn't execute inline. Instead, it adds the SQL to be executed
  in a batch when you call `IDocumentSession.SaveChangesAsync()`. This batching reduces network round trips to the
  database and is a consistent performance win.
* **Event metadata access** -- The `Project()` methods use `IEvent<T>` envelope types, giving you access to event metadata
  like timestamps, version information, and stream identity. This is something the declarative `FlatTableProjection`
  cannot currently provide.
* **Full SQL control** -- You can write any SQL you need: inserts, updates, deletes, or even complex statements with
  subqueries. This is useful when your projection logic doesn't fit the `Map`/`Increment`/`SetValue` patterns of
  `FlatTableProjection`.
