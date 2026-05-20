using JasperFx;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Versioning;

/// <summary>
///     Decision D2 widens the document `version` column to bigint always. Tables created before this
///     change carry an int column; the next schema migration must ALTER it in place rather than
///     drop/recreate it (which would lose every document). This test stands in for that upgrade by
///     shrinking a freshly-created table's version column back to int, then proving a fresh store's
///     lazy table-ensure migration widens it to bigint while the seeded row survives.
/// </summary>
[Collection("integration")]
public class version_column_widening_migration : IntegrationContext
{
    private const string Schema = "version_widening";
    private const string Table = "[version_widening].[pc_doc_user]";

    public version_column_widening_migration(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = Schema);
    }

    [Fact]
    public async Task widens_int_version_column_to_bigint_without_dropping_data()
    {
        // Seed a row through the normal path (creates the table with a bigint version column).
        var seed = new User { Id = Guid.NewGuid(), FirstName = "seed" };
        theSession.Store(seed);
        await theSession.SaveChangesAsync();

        // Recreate the pre-D2 schema exactly: an int version column carrying a DEFAULT 1 constraint
        // (which is itself what blocks a naive ALTER COLUMN). Drop the bigint default, shrink to int,
        // restore the default — leaving the table looking like one created by a released Polecat.
        await using (var conn = await OpenConnectionAsync())
        {
            await using var alter = conn.CreateCommand();
            alter.CommandText = $"""
                DECLARE @oid int = OBJECT_ID('{Table}');
                DECLARE @df sysname, @sql nvarchar(max);
                SELECT @df = dc.name FROM sys.default_constraints dc
                    JOIN sys.columns col ON col.default_object_id = dc.object_id
                    WHERE dc.parent_object_id = @oid AND col.name = 'version';
                IF @df IS NOT NULL
                BEGIN
                    SET @sql = 'ALTER TABLE {Table} DROP CONSTRAINT ' + QUOTENAME(@df);
                    EXEC(@sql);
                END
                ALTER TABLE {Table} ALTER COLUMN version int NOT NULL;
                ALTER TABLE {Table} ADD DEFAULT 1 FOR version;
                """;
            await alter.ExecuteNonQueryAsync();
        }

        (await VersionColumnTypeAsync()).ShouldBe("int");

        // A fresh store starts with an empty table-ensure cache, so the next SaveChanges runs a
        // Weasel SchemaMigration against the int column — which must widen it in place.
        var opts = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            AutoCreateSchemaObjects = AutoCreate.All,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson,
            DatabaseSchemaName = Schema
        };

        await using var store2 = new DocumentStore(opts);
        await using (var session2 = store2.LightweightSession())
        {
            session2.Store(new User { Id = Guid.NewGuid(), FirstName = "second" });
            await session2.SaveChangesAsync();
        }

        // The column is bigint again — proving an ALTER, not a drop/recreate...
        (await VersionColumnTypeAsync()).ShouldBe("bigint");

        // ...and the row seeded before the widening is still there.
        await using var query = store2.QuerySession();
        var loaded = await query.LoadAsync<User>(seed.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("seed");
    }

    private async Task<string?> VersionColumnTypeAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = 'pc_doc_user' AND COLUMN_NAME = 'version';";
        cmd.Parameters.AddWithValue("@schema", Schema);
        var result = await cmd.ExecuteScalarAsync();
        return result as string;
    }
}
