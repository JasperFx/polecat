using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Tests.Storage;

/// <summary>
///     SqlServerStorageDialect is the SQL Server IStorageDialect of the shared closed-shape
///     storage runtime (#273 phase D): named-parameter load commands, OPENJSON id arrays,
///     StorageColumnType → SqlDbType mapping, undefined-table detection.
/// </summary>
public class sqlserver_storage_dialect_tests : OneOffConfigurationsContext
{
    private readonly IStorageDialect theDialect = SqlServerStorageDialect<Guid>.Instance;

    [Fact]
    public void build_load_command_binds_id_and_tenant()
    {
        var id = Guid.NewGuid();
        var command = theDialect.BuildLoadCommand("SELECT data FROM t WHERE id = @id AND tenant_id = @tenant_id", id, "t1");

        var sql = command.ShouldBeOfType<SqlCommand>();
        sql.Parameters.Count.ShouldBe(2);
        sql.Parameters["id"].Value.ShouldBe(id);
        sql.Parameters["id"].SqlDbType.ShouldBe(SqlDbType.UniqueIdentifier);
        sql.Parameters["tenant_id"].Value.ShouldBe("t1");
    }

    [Fact]
    public void build_load_command_omits_tenant_when_null()
    {
        var command = theDialect.BuildLoadCommand("SELECT 1", Guid.NewGuid(), null);
        command.Parameters.Count.ShouldBe(1);
    }

    [Theory]
    [InlineData(StorageColumnType.String, SqlDbType.NVarChar)]
    [InlineData(StorageColumnType.Guid, SqlDbType.UniqueIdentifier)]
    [InlineData(StorageColumnType.Long, SqlDbType.BigInt)]
    [InlineData(StorageColumnType.Int, SqlDbType.Int)]
    [InlineData(StorageColumnType.Boolean, SqlDbType.Bit)]
    [InlineData(StorageColumnType.Timestamp, SqlDbType.DateTimeOffset)]
    [InlineData(StorageColumnType.Json, SqlDbType.NVarChar)]
    public void set_parameter_type_maps_storage_column_types(StorageColumnType storageType, SqlDbType expected)
    {
        var parameter = new SqlParameter();
        theDialect.SetParameterType(parameter, storageType);
        parameter.SqlDbType.ShouldBe(expected);
    }

    [Fact]
    public void set_id_parameter_type_uses_the_provider_mapping()
    {
        var parameter = new SqlParameter();
        theDialect.SetIdParameterType(parameter, typeof(long));
        parameter.SqlDbType.ShouldBe(SqlDbType.BigInt);
    }

    [Fact]
    public void by_id_filter_renders_the_id_predicate()
    {
        var id = Guid.NewGuid();
        var fragment = theDialect.ByIdFilter(id);

        var builder = new CommandBuilder();
        fragment.Apply(builder);
        var command = builder.Compile();

        command.CommandText.ShouldBe("id = @p0");
        command.Parameters[0].Value.ShouldBe(id);
        command.Parameters[0].SqlDbType.ShouldBe(SqlDbType.UniqueIdentifier);
    }

    [Fact]
    public async Task is_undefined_table_detects_error_208()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM dbo.pc_no_such_table_ever";

        var ex = await Should.ThrowAsync<SqlException>(() => cmd.ExecuteScalarAsync());
        theDialect.IsUndefinedTable(ex).ShouldBeTrue();
        theDialect.IsUndefinedTable(new InvalidOperationException("nope")).ShouldBeFalse();
    }

    [Fact]
    public async Task openjson_id_array_round_trips_guids_against_sql_server()
    {
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var parameter = theDialect.CreateIdArrayParameter(ids, typeof(Guid));

        var command = theDialect.BuildLoadManyCommand(
            "SELECT value FROM OPENJSON(@ids)", parameter, null);

        await using var conn = await OpenConnectionAsync();
        command.Connection = (SqlConnection)conn;

        var returned = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            returned.Add(Guid.Parse(reader.GetString(0)));
        }

        returned.ShouldBe(ids);
    }

    [Fact]
    public async Task openjson_id_array_round_trips_longs_and_strings()
    {
        var longDialect = SqlServerStorageDialect<long>.Instance;
        var longParam = longDialect.CreateIdArrayParameter(new[] { 1L, 42L, long.MaxValue }, typeof(long));

        var stringDialect = SqlServerStorageDialect<string>.Instance;
        var stringParam = stringDialect.CreateIdArrayParameter(new[] { "a", "quo\"te", "c" }, typeof(string));

        await using var conn = await OpenConnectionAsync();

        await using (var cmd = new SqlCommand("SELECT CAST(value AS BIGINT) FROM OPENJSON(@ids)", (SqlConnection)conn))
        {
            cmd.Parameters.Add((SqlParameter)longParam);
            var values = new List<long>();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) values.Add(reader.GetInt64(0));
            values.ShouldBe([1L, 42L, long.MaxValue]);
        }

        await using (var cmd = new SqlCommand("SELECT value FROM OPENJSON(@ids)", (SqlConnection)conn))
        {
            cmd.Parameters.Add((SqlParameter)stringParam);
            var values = new List<string>();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) values.Add(reader.GetString(0));
            values.ShouldBe(["a", "quo\"te", "c"]);
        }
    }

    [Fact]
    public void id_array_parameter_rejects_unsupported_id_types()
    {
        Should.Throw<ArgumentOutOfRangeException>(() =>
            theDialect.CreateIdArrayParameter(new[] { 1.5, 2.5 }, typeof(double)));
    }
}
