using JasperFx;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Projections.Flattened;
using Polecat.Linq;
using Polecat.Storage;
using Polecat.TestUtils;

namespace Polecat.Tests.Storage;

/// <summary>
///     #665. <see cref="SqlEscaping" /> documents itself as the single place Polecat escapes a value
///     it is about to interpolate into SQL, and that claim was not true everywhere — a handful of
///     sites escaped correctly by hand instead. None was believed exploitable; the reason to close
///     them is that "escaped correctly by hand" is exactly the state Marten's full-text code was in
///     right before a new code path quietly skipped the check.
/// </summary>
/// <remarks>
///     These are companions to <see cref="sql_escaping_tests" />, which unit-tests the helper itself.
///     These test the SITES.
/// </remarks>
public class sql_escaping_boundary_tests
{
    public class TenantedDoc
    {
        public Guid Id { get; set; }
        public string? Name { get; set; }
    }

    private static DocumentStore ConjoinedStore(string schema)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });
    }

    /// <summary>
    ///     Item 1, and the only one of the four whose value is genuinely runtime-reachable from a
    ///     public API: <c>SessionOptions.TenantId</c> is commonly a header or a claim. It used to land
    ///     in a single-quoted literal built with <c>TenantId.Replace("'", "''")</c>.
    /// </summary>
    [Fact]
    public async Task the_tenant_filter_binds_a_parameter_rather_than_interpolating_the_tenant_id()
    {
        using var store = ConjoinedStore("escape_tenant");
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(
            ct: TestContext.Current.CancellationToken);

        await using var query = store.QuerySession(new SessionOptions { TenantId = "o'brien" });
        var sql = query.ToSql(query.Query<TenantedDoc>().Where(x => x.Name == "x"));

        sql.ShouldContain("tenant_id");

        // The mechanism, not just the outcome: the value is bound, so it is nowhere in the text —
        // neither raw nor hand-escaped.
        sql.ShouldNotContain("o'brien");
        sql.ShouldNotContain("o''brien");
    }

    /// <summary>
    ///     And the behaviour the binding has to preserve: a quote in a tenant id is data, and tenants
    ///     stay isolated. This passed before the change too — the hand-escaping was correct — which is
    ///     why the fact above asserts the mechanism as well.
    /// </summary>
    [Fact]
    public async Task a_tenant_id_containing_a_quote_still_isolates_its_documents()
    {
        using var store = ConjoinedStore("escape_tenant_rt");
        var token = TestContext.Current.CancellationToken;
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        // The suite shares one database and this schema's table survives between runs, so rows from
        // an earlier run would otherwise be counted as this one's. Found the hard way: the first
        // draft of this fact read 2 documents for a tenant it had stored one for.
        await store.Advanced.Clean.DeleteAllDocumentsAsync(token);

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "o'brien" }))
        {
            session.Store(new TenantedDoc { Id = Guid.NewGuid(), Name = "quoted" });
            await session.SaveChangesAsync(token);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "plain" }))
        {
            session.Store(new TenantedDoc { Id = Guid.NewGuid(), Name = "unquoted" });
            await session.SaveChangesAsync(token);
        }

        await using (var query = store.QuerySession(new SessionOptions { TenantId = "o'brien" }))
        {
            var mine = await query.Query<TenantedDoc>().ToListAsync(token);
            mine.ShouldHaveSingleItem().Name.ShouldBe("quoted");
        }

        await using (var query = store.QuerySession(new SessionOptions { TenantId = "plain" }))
        {
            var mine = await query.Query<TenantedDoc>().ToListAsync(token);
            mine.ShouldHaveSingleItem().Name.ShouldBe("unquoted");
        }
    }

    /// <summary>
    ///     Item 2: the master-tenant table's <c>CREATE SCHEMA</c> bound <c>@schema</c> as a parameter
    ///     and then concatenated its VALUE into the EXEC'd batch as <c>'[' + @schema + ']'</c>, so a
    ///     <c>]</c> closed the bracket early inside the nested batch. A parameter being involved is
    ///     not the same as the value being escaped, and this was the one place those came apart.
    /// </summary>
    [Fact]
    public async Task a_master_table_schema_name_containing_a_bracket_is_provisioned_correctly()
    {
        var token = TestContext.Current.CancellationToken;
        const string schema = "esc]ape_tenants";

        await DropSchemaAsync(schema, token);

        try
        {
            using var store = DocumentStore.For(opts =>
            {
                opts.AutoCreateSchemaObjects = AutoCreate.All;
                opts.MultiTenantedMasterTable(ConnectionSource.ConnectionString, schema);
            });

            var tenancy = (MasterTableTenancy)store.Options.Tenancy!;

            // Provisioning the registry is what runs the EXEC'd CREATE SCHEMA. Before #665 the
            // bracket broke the nested batch.
            await tenancy.BuildDatabasesAsync(token);

            (await SchemaExistsAsync(schema, token)).ShouldBeTrue();
        }
        finally
        {
            await DropSchemaAsync(schema, token);
        }
    }

    /// <summary>
    ///     Item 4: six of the seven flat-table column maps bracketed their column name by hand while
    ///     the seventh went through <see cref="SqlEscaping" /> and cited #390 for why. Config-time
    ///     names and parameterized values either way — but one file doing it two ways is what makes
    ///     the next reader guess which was deliberate.
    /// </summary>
    [Fact]
    public void every_flat_table_column_map_escapes_its_column_name()
    {
        const string column = "we]ird";
        const string expected = "[we]]ird]";

        var maps = new IColumnMap[]
        {
            new MemberMap(column),
            new IncrementMemberMap(column),
            new DecrementMemberMap(column),
            new IncrementMap(column),
            new DecrementMap(column),
            new SetStringValueMap(column, "v"),
            new SetIntValueMap(column, 7)
        };

        foreach (var map in maps)
        {
            map.UpdateExpression("@p0").ShouldStartWith(expected);
        }
    }

    private static async Task<bool> SchemaExistsAsync(string schema, CancellationToken token)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sys.schemas WHERE name = @name";
        cmd.Parameters.AddWithValue("@name", schema);
        return (int)(await cmd.ExecuteScalarAsync(token))! > 0;
    }

    private static async Task DropSchemaAsync(string schema, CancellationToken token)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();

        // QUOTENAME here for the same reason the production site now uses it.
        cmd.CommandText = """
            DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql = @sql + N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';'
            FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @name;
            IF SCHEMA_ID(@name) IS NOT NULL
                SET @sql = @sql + N'DROP SCHEMA ' + QUOTENAME(@name) + N';';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@name", schema);
        await cmd.ExecuteNonQueryAsync(token);
    }
}
