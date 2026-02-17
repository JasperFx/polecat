using Microsoft.Data.SqlClient;

namespace Polecat.Tests.Harness;

/// <summary>
///     SQL Server schema introspection helpers for integration tests.
///     Queries INFORMATION_SCHEMA and sys views to verify table structures.
/// </summary>
internal static class SchemaInspector
{
    public static async Task<List<string>> GetTableNamesAsync(string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = @schema AND TABLE_TYPE = 'BASE TABLE'
            """;
        cmd.Parameters.AddWithValue("@schema", schema);

        var tables = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
    }

    public static async Task<List<ColumnInfo>> GetColumnInfoAsync(string tableName, string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", tableName);

        var columns = new List<ColumnInfo>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(new ColumnInfo(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2) == "YES",
                reader.IsDBNull(3) ? null : reader.GetInt32(3)));
        }

        return columns;
    }

    public static async Task<bool> IsColumnIdentityAsync(string tableName, string columnName,
        string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COLUMNPROPERTY(OBJECT_ID(@table), @column, 'IsIdentity')";
        cmd.Parameters.AddWithValue("@table", $"{schema}.{tableName}");
        cmd.Parameters.AddWithValue("@column", columnName);

        var result = await cmd.ExecuteScalarAsync();
        return result is 1;
    }

    public static async Task<List<IndexInfo>> GetIndexInfoAsync(string tableName, string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT i.name, i.is_unique
            FROM sys.indexes i
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema AND t.name = @table AND i.name IS NOT NULL
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", tableName);

        var indexes = new List<IndexInfo>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            indexes.Add(new IndexInfo(reader.GetString(0), reader.GetBoolean(1)));
        }

        return indexes;
    }

    public static async Task<List<string>> GetIndexColumnsAsync(string tableName, string indexName,
        string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT c.name
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema AND t.name = @table AND i.name = @index
            ORDER BY ic.key_ordinal
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", tableName);
        cmd.Parameters.AddWithValue("@index", indexName);

        var columns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    public static async Task<List<string>> GetPrimaryKeyColumnsAsync(string tableName, string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
            WHERE tc.TABLE_SCHEMA = @schema AND tc.TABLE_NAME = @table AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY c.ORDINAL_POSITION
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", tableName);

        var columns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    public static async Task<List<ForeignKeyInfo>> GetForeignKeysAsync(string tableName, string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT fk.name, OBJECT_NAME(fk.referenced_object_id) as referenced_table
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema AND t.name = @table
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", tableName);

        var fks = new List<ForeignKeyInfo>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            fks.Add(new ForeignKeyInfo(reader.GetString(0), reader.GetString(1)));
        }

        return fks;
    }

    public static async Task DropEventStoreTablesAsync(string schema = "dbo")
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID('{schema}.pc_events', 'U') IS NOT NULL DROP TABLE [{schema}].pc_events;
            IF OBJECT_ID('{schema}.pc_streams', 'U') IS NOT NULL DROP TABLE [{schema}].pc_streams;
            IF OBJECT_ID('{schema}.pc_event_progression', 'U') IS NOT NULL DROP TABLE [{schema}].pc_event_progression;
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    public record ColumnInfo(string Name, string TypeName, bool IsNullable, int? MaxLength);

    public record IndexInfo(string Name, bool IsUnique);

    public record ForeignKeyInfo(string Name, string ReferencedTable);
}
