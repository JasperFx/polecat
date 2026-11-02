using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Schema;

public record UndeclaredTick(Guid Id);

public class UndeclaredLog
{
    public Guid Id { get; set; }
    public string Code { get; set; } = "";
}

public class DeclaredLog
{
    public Guid Id { get; set; }
}

/// <summary>
///     The ordinary shape of an event projection: <c>Project</c> writes whatever it writes, and nothing
///     about the registration says what that is.
/// </summary>
public partial class UndeclaredWriteProjection : EventProjection
{
    public UndeclaredWriteProjection()
    {
        Name = "UndeclaredWriteProjection";
    }

    public void Project(UndeclaredTick e, IDocumentSession ops)
    {
        ops.Store(new UndeclaredLog { Id = e.Id });
    }
}

public partial class DeclaredWriteProjection : EventProjection
{
    public DeclaredWriteProjection()
    {
        Name = "DeclaredWriteProjection";
        RegisterPublishedType(typeof(DeclaredLog));
    }

    public void Project(UndeclaredTick e, IDocumentSession ops)
    {
        ops.Store(new DeclaredLog { Id = e.Id });
    }
}

/// <summary>
///     #870: the ensure <see cref="first_use_schema_lock_tests" /> calls the one a save cannot move ahead
///     of its transaction. It needed a connection of its own for that, a second one for a session already
///     holding a transaction, which is a stall at the limit of the pool and impossible on a pool of one.
/// </summary>
public class first_use_ensure_inside_a_save_tests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string NewSchema() => "fsi_" + Guid.NewGuid().ToString("N")[..10];

    private static string PoolOfOne()
    {
        return new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            MaxPoolSize = 1,
            ConnectTimeout = 3,
            MultipleActiveResultSets = false,

            // A pool is keyed on the connection string, so this keeps each store's pool to itself.
            ApplicationName = $"fsi_{Guid.NewGuid():N}"
        }.ConnectionString;
    }

    private static DocumentStore BuildStore(string connectionString, string schema, Action<StoreOptions> configure)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            configure(opts);
        });
    }

    private static void Undeclared(StoreOptions opts)
    {
        opts.Projections.Add(new UndeclaredWriteProjection(), ProjectionLifecycle.Inline);
    }

    private static void Declared(StoreOptions opts)
    {
        opts.Projections.Add(new DeclaredWriteProjection(), ProjectionLifecycle.Inline);
    }

    private static async Task<Guid> SaveAsync(IDocumentStore store)
    {
        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new UndeclaredTick(id));
        await session.SaveChangesAsync(Ct);
        return id;
    }

    /// <summary>
    ///     What every process start used to meet: the table has been there all along, and this store has
    ///     just not asked yet.
    /// </summary>
    [Fact]
    public async Task a_table_that_exists_needs_no_second_connection()
    {
        var schema = NewSchema();

        using (var earlier = BuildStore(ConnectionSource.ConnectionString, schema, Undeclared))
        {
            await SaveAsync(earlier);
        }

        using var store = BuildStore(PoolOfOne(), schema, Undeclared);

        var id = await SaveAsync(store);

        await using var query = store.QuerySession();
        (await query.LoadAsync<UndeclaredLog>(id, Ct)).ShouldNotBeNull();
    }

    /// <summary>
    ///     Finding the table is not ensuring it. The index below is new since the table was created, so
    ///     only the whole ensure can put it there, and that is left to the next save, ahead of its
    ///     transaction. From then on the store knows, and neither looks again nor does the work again.
    /// </summary>
    [Fact]
    public async Task the_next_save_does_the_rest_of_the_ensure()
    {
        var schema = NewSchema();

        using (var earlier = BuildStore(ConnectionSource.ConnectionString, schema, Undeclared))
        {
            await SaveAsync(earlier);
        }

        var before = await IndexCountAsync(schema);

        using var store = BuildStore(ConnectionSource.ConnectionString, schema, opts =>
        {
            Undeclared(opts);
            opts.Schema.For<UndeclaredLog>().Index(x => x.Code);
        });

        await SaveAsync(store);

        (await IndexCountAsync(schema)).ShouldBe(before);

        await SaveAsync(store);

        (await IndexCountAsync(schema)).ShouldBeGreaterThan(before);

        var ensures = EnsurerOf(store).SchemaEnsureExecutions;

        await SaveAsync(store);

        EnsurerOf(store).SchemaEnsureExecutions.ShouldBe(ensures);
    }

    private static DocumentTableEnsurer EnsurerOf(DocumentStore store)
    {
        return store.EnsurerFor(store.Database.ConnectionString);
    }

    private static async Task<int> IndexCountAsync(string schema)
    {
        await using var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = new SqlCommand(
            "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(@table);", connection);
        command.Parameters.AddWithValue("@table", $"{schema}.pc_doc_undeclaredlog");

        return (int)(await command.ExecuteScalarAsync(Ct))!;
    }

    /// <summary>
    ///     A table that really is missing still gets created from inside the save, as long as the pool
    ///     can spare the connection.
    /// </summary>
    [Fact]
    public async Task a_missing_table_is_still_created()
    {
        using var store = BuildStore(ConnectionSource.ConnectionString, NewSchema(), Undeclared);

        var id = await SaveAsync(store);

        await using var query = store.QuerySession();
        (await query.LoadAsync<UndeclaredLog>(id, Ct)).ShouldNotBeNull();
    }

    /// <summary>
    ///     The way out of what is left. A projection that says what it writes is ensured ahead of the
    ///     transaction, so even a brand new table needs one connection at a time.
    /// </summary>
    [Fact]
    public async Task a_declared_type_needs_no_second_connection_even_when_its_table_is_missing()
    {
        var schema = NewSchema();

        using (var earlier = BuildStore(ConnectionSource.ConnectionString, schema, _ => { }))
        {
            await using var seed = earlier.LightweightSession();
            seed.Events.StartStream(Guid.NewGuid(), new UndeclaredTick(Guid.NewGuid()));
            await seed.SaveChangesAsync(Ct);
        }

        using var store = BuildStore(PoolOfOne(), schema, Declared);

        var id = await SaveAsync(store);

        await using var query = store.QuerySession();
        (await query.LoadAsync<DeclaredLog>(id, Ct)).ShouldNotBeNull();
    }
}
