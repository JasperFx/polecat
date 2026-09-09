using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Storage;

/// <summary>
///     Every read on a query session that runs through a <see cref="SqlBatch" /> — document LINQ,
///     event LINQ, batched queries, session loads — must return its pooled connection when the reader
///     is disposed.
/// </summary>
/// <remarks>
///     <para>
///         <c>AutoClosingLifetime</c> asked for <c>CommandBehavior.CloseConnection</c> on both of its
///         reader paths and relied on it for both. Microsoft.Data.SqlClient honours it for a
///         <see cref="SqlCommand" /> reader and <b>silently ignores it</b> for a <see cref="SqlBatch" />
///         one, so the batch path leaked a pooled connection per query.
///     </para>
///     <para>
///         Why it went unnoticed: the default max pool size is 100, and a process reaches that only
///         after a hundred queries on one connection string. When it did, the failure named neither the
///         leak nor the query that caused it — just "Timeout expired" on whichever unlucky query
///         crossed the line, in whichever class happened to be longest that day. These tests pin the
///         connection accounting directly, against a pool small enough that a single leak is fatal, so
///         a regression fails here and says what broke.
///     </para>
/// </remarks>
public class batch_reader_connection_release_tests
{
    /// <summary>
    ///     A pool of 3 against 25 queries: any per-query leak exhausts it long before the end. The
    ///     distinct application name gives each test its own pool, so one test's leak cannot be
    ///     mistaken for another's.
    /// </summary>
    private static string SmallPool(string name) =>
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            MaxPoolSize = 3, ConnectTimeout = 5, ApplicationName = name
        }.ConnectionString;

    private static DocumentStore StoreOn(string connectionString, string schema) =>
        DocumentStore.For(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

    /// <summary>
    ///     The ADO.NET fact the fix rests on, asserted rather than assumed.
    /// </summary>
    /// <remarks>
    ///     Written to document the driver's behaviour, not to demand it stay broken: if a future
    ///     Microsoft.Data.SqlClient starts honouring the flag for batches, this is the one test that
    ///     fails, its message says the decorator is now redundant, and every other test in the class
    ///     keeps passing.
    /// </remarks>
    [Fact]
    public async Task sql_batch_does_not_honour_close_connection_so_polecat_must_close_it()
    {
        var cs = SmallPool("polecat_batch_behaviour_probe");

        var leaked = false;
        try
        {
            for (var i = 0; i < 25; i++)
            {
                var conn = new SqlConnection(cs);
                await conn.OpenAsync(TestContext.Current.CancellationToken);
                await using var batch = new SqlBatch { Connection = conn };
                batch.BatchCommands.Add(new SqlBatchCommand("select 1"));
                await using var reader = await batch.ExecuteReaderAsync(
                    System.Data.CommandBehavior.CloseConnection, TestContext.Current.CancellationToken);
                while (await reader.ReadAsync(TestContext.Current.CancellationToken)) { }
            }
        }
        catch (InvalidOperationException e) when (e.Message.Contains("pool"))
        {
            leaked = true;
        }

        leaked.ShouldBeTrue(
            "SqlBatch appears to honour CommandBehavior.CloseConnection now — ConnectionClosingDataReader " +
            "is no longer load-bearing, though it remains correct.");
    }

    /// <summary>
    ///     The SqlCommand control: the same loop on the path where the flag <em>is</em> honoured, so a
    ///     failure above can be read as "batches differ" rather than "the pool is too small".
    /// </summary>
    [Fact]
    public async Task sql_command_does_honour_close_connection()
    {
        var cs = SmallPool("polecat_command_behaviour_probe");

        for (var i = 0; i < 25; i++)
        {
            var conn = new SqlConnection(cs);
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            var cmd = new SqlCommand("select 1", conn);
            await using var reader = await cmd.ExecuteReaderAsync(
                System.Data.CommandBehavior.CloseConnection, TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken)) { }
        }
    }

    [Fact]
    public async Task repeated_document_linq_queries_do_not_exhaust_the_pool()
    {
        using var store = StoreOn(SmallPool("polecat_doc_linq_pool"), "pool_doc_linq");

        await using (var session = store.LightweightSession())
        {
            session.Store(new PoolTarget { Id = Guid.NewGuid(), Name = "seed" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        for (var i = 0; i < 25; i++)
        {
            await using var query = store.QuerySession();
            var all = await query.Query<PoolTarget>().ToListAsync(TestContext.Current.CancellationToken);
            all.Count.ShouldBeGreaterThan(0);
        }
    }

    [Fact]
    public async Task repeated_event_queries_do_not_exhaust_the_pool()
    {
        using var store = StoreOn(SmallPool("polecat_event_query_pool"), "pool_event_query");

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted("seed"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // OpenReadOnlyEventStore() per call is how both the Event Store Explorer and
        // EventQueryCompliance use it, and it is the shape that first hit the ceiling.
        for (var i = 0; i < 25; i++)
        {
            var page = await ((IEventStore)store).OpenReadOnlyEventStore()
                .QueryEventsAsync(new EventQuery { PageSize = 10 }, TestContext.Current.CancellationToken);
            page.TotalCount.ShouldBeGreaterThan(0);
        }
    }

    [Fact]
    public async Task repeated_session_loads_do_not_exhaust_the_pool()
    {
        using var store = StoreOn(SmallPool("polecat_session_load_pool"), "pool_session_load");

        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Store(new PoolTarget { Id = id, Name = "seed" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        for (var i = 0; i < 25; i++)
        {
            await using var query = store.QuerySession();
            var loaded = await query.LoadAsync<PoolTarget>(id, TestContext.Current.CancellationToken);
            loaded.ShouldNotBeNull();
        }
    }
}

public class PoolTarget
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
