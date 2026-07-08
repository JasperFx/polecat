using Microsoft.Data.SqlClient;
using Polecat.TestUtils;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

// #296: strongly-typed id documents must persist the id column as the INNER primitive type
// (uniqueidentifier / int / bigint), not varchar(250) derived from the wrapper type. Otherwise the
// shared writeable selectors — used by Lightweight/IdentityMap database reads — throw
// InvalidCastException when they read the inner value via GetFieldValue<TInner> against a varchar
// column. QueryOnly reads (QuerySession) exclude the id column and so never tripped this, which is
// why the earlier smoke tests loaded through a QuerySession and stayed green.
[Collection("integration")]
public class strong_typed_id_column_type_tests : IntegrationContext
{
    public strong_typed_id_column_type_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "strong_id_column"; });
    }

    [Fact]
    public async Task guid_wrapper_id_column_is_uniqueidentifier()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Invoice { Name = "Schema" });
            await session.SaveChangesAsync();
        }

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_invoice", "strong_id_column");
        var id = columns.Single(c => c.Name == "id");
        id.TypeName.ShouldBe("uniqueidentifier");
    }

    [Fact]
    public async Task int_wrapper_id_column_is_int()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new OrderItem { Name = "Schema" });
            await session.SaveChangesAsync();
        }

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_orderitem", "strong_id_column");
        columns.Single(c => c.Name == "id").TypeName.ShouldBe("int");
    }

    [Fact]
    public async Task long_wrapper_id_column_is_bigint()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Issue { Name = "Schema" });
            await session.SaveChangesAsync();
        }

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_issue", "strong_id_column");
        columns.Single(c => c.Name == "id").TypeName.ShouldBe("bigint");
    }

    [Fact]
    public async Task string_wrapper_id_column_stays_varchar()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Team { Id = new TeamId("t-1"), Name = "Schema" });
            await session.SaveChangesAsync();
        }

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_team", "strong_id_column");
        columns.Single(c => c.Name == "id").TypeName.ShouldBe("varchar");
    }

    // The core regression: a Lightweight database read (fresh session, empty identity map so the
    // read must hit the writeable selector) of a wrapper-id document previously threw
    // InvalidCastException. With the inner-typed id column it round-trips.
    [Fact]
    public async Task lightweight_database_read_of_guid_wrapper_id_round_trips()
    {
        var invoice = new Invoice { Name = "Loaded" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(invoice);
            await session.SaveChangesAsync();
        }

        await using var fresh = theStore.LightweightSession();
        var loaded = await fresh.LoadAsync<Invoice>(invoice.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Loaded");
    }

    [Fact]
    public async Task lightweight_database_read_of_int_wrapper_id_round_trips()
    {
        var item = new OrderItem { Name = "Loaded" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(item);
            await session.SaveChangesAsync();
        }

        await using var fresh = theStore.LightweightSession();
        var loaded = await fresh.LoadAsync<OrderItem>(item.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Loaded");
    }

    [Fact]
    public async Task lightweight_database_read_of_long_wrapper_id_round_trips()
    {
        var issue = new Issue { Name = "Loaded" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(issue);
            await session.SaveChangesAsync();
        }

        await using var fresh = theStore.LightweightSession();
        var loaded = await fresh.LoadAsync<Issue>(issue.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Loaded");
    }

    // The migration story: a legacy table created before #296 carries a varchar(250) id column
    // holding the guid as a string. The first document access (which runs the lazy schema ensure
    // before any read/write — see QuerySession.LoadInternalAsync) converts it to uniqueidentifier
    // in place (drop PK, ALTER COLUMN, re-add PK) — data preserved — so the pre-existing row is then
    // readable through the Lightweight writeable selector. (Weasel's bulk ApplyAll leaves the PK
    // column type alone; the conversion is the ensurer's job and always precedes real document I/O.)
    [Fact]
    public async Task migrates_legacy_varchar_id_column_in_place_preserving_data()
    {
        const string schema = "strong_id_migrate";
        var legacyId = new Guid("11111111-1111-1111-1111-111111111111");

        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync();
            await ExecuteAsync(conn, $"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA {schema}')");
            await ExecuteAsync(conn, $"DROP TABLE IF EXISTS {schema}.pc_doc_invoice");
            // Legacy shape: id is varchar(250) as pre-#296 tables were created.
            await ExecuteAsync(conn, $"""
                CREATE TABLE {schema}.pc_doc_invoice (
                    id varchar(250) NOT NULL,
                    data json NOT NULL,
                    version bigint NOT NULL,
                    last_modified datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                    created_at datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                    dotnet_type varchar(500) NULL,
                    tenant_id varchar(250) NOT NULL DEFAULT '*DEFAULT*',
                    CONSTRAINT pkey_pc_doc_invoice_id PRIMARY KEY (id));
                """);
            // Polecat serializes with CamelCase by default, so the legacy body uses camelCase keys.
            var json = "{\"id\":{\"value\":\"" + legacyId + "\"},\"name\":\"Legacy\"}";
            await ExecuteAsync(conn,
                $"INSERT INTO {schema}.pc_doc_invoice (id, data, version) VALUES ('{legacyId}', '{json}', 1);");
        }

        await StoreOptions(opts => { opts.DatabaseSchemaName = schema; });

        // First document access triggers the lazy ensure, which converts the id column in place,
        // then loads the pre-existing row through the Lightweight writeable selector.
        await using var session = theStore.LightweightSession();
        var loaded = await session.LoadAsync<Invoice>(legacyId);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Legacy");

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_invoice", schema);
        columns.Single(c => c.Name == "id").TypeName.ShouldBe("uniqueidentifier");
    }

    private static async Task ExecuteAsync(SqlConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
