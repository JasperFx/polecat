using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore.Tests;

public static class EfCoreTestHelper
{
    /// <summary>
    ///     Ensures EF Core tables exist in the database using a fresh DbContext.
    /// </summary>
    public static async Task EnsureEfCoreTablesAsync<TDbContext>(string connectionString)
        where TDbContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TDbContext>();
        optionsBuilder.UseSqlServer(connectionString);

        await using var dbContext =
            (TDbContext)Activator.CreateInstance(typeof(TDbContext), optionsBuilder.Options)!;
        var script = dbContext.Database.GenerateCreateScript();

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        // Execute each statement separately (CREATE TABLE statements separated by GO)
        foreach (var statement in script.Split(["\r\nGO\r\n", "\nGO\n", "\nGO\r\n"],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = statement.Trim();
            if (string.IsNullOrEmpty(trimmed)) continue;

            try
            {
                await using var cmd = new SqlCommand(trimmed, conn);
                await cmd.ExecuteNonQueryAsync();
            }
            catch (SqlException ex) when (ex.Number == 2714) // Object already exists
            {
                // Table already exists, skip
            }
        }
    }

    /// <summary>
    ///     Drop and recreate EF Core tables for a clean test slate.
    /// </summary>
    public static async Task CleanEfCoreTablesAsync(string connectionString, params string[] tableNames)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        foreach (var table in tableNames)
        {
            try
            {
                await using var cmd = new SqlCommand($"DELETE FROM {table};", conn);
                await cmd.ExecuteNonQueryAsync();
            }
            catch (SqlException)
            {
                // Table might not exist yet, ignore
            }
        }
    }

    /// <summary>
    ///     Create a DocumentStore configured with an EF Core single-stream projection.
    /// </summary>
    public static async Task<DocumentStore> CreateStoreWithSingleStreamProjection(ProjectionLifecycle lifecycle)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.DatabaseSchemaName = $"efcore_ss_{lifecycle.ToString().ToLowerInvariant()}";

            opts.Projections.Add<OrderAggregate, Order, TestDbContext>(
                opts, new OrderAggregate(), lifecycle);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_orders", "ef_order_summaries");
        return store;
    }

    /// <summary>
    ///     Create a DocumentStore configured with an EF Core multi-stream projection.
    /// </summary>
    public static async Task<DocumentStore> CreateStoreWithMultiStreamProjection(ProjectionLifecycle lifecycle)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.DatabaseSchemaName = $"efcore_ms_{lifecycle.ToString().ToLowerInvariant()}";

            opts.Projections.Add<CustomerOrderHistoryProjection, CustomerOrderHistory, string, TestDbContext>(
                opts, new CustomerOrderHistoryProjection(), lifecycle);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_customer_order_histories");
        return store;
    }

    /// <summary>
    ///     Create a DocumentStore configured with an EF Core event projection.
    /// </summary>
    public static async Task<DocumentStore> CreateStoreWithEventProjection(ProjectionLifecycle lifecycle)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.DatabaseSchemaName = $"efcore_ep_{lifecycle.ToString().ToLowerInvariant()}";

            opts.Projections.Add<OrderDetailProjection, TestDbContext>(
                opts, new OrderDetailProjection(), lifecycle);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await CleanEfCoreTablesAsync(ConnectionSource.ConnectionString,
            "ef_order_details", $"efcore_ep_{lifecycle.ToString().ToLowerInvariant()}.pc_doc_orderlog");
        return store;
    }

    /// <summary>
    ///     Read a value from the database using raw SQL.
    /// </summary>
    public static async Task<T?> QueryScalarAsync<T>(string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        foreach (var (name, value) in parameters) cmd.Parameters.AddWithValue(name, value);

        var result = await cmd.ExecuteScalarAsync();
        if (result is null or DBNull) return default;
        return (T)Convert.ChangeType(result, typeof(T));
    }

    /// <summary>
    ///     Read a row from the database using raw SQL.
    /// </summary>
    public static async Task<Dictionary<string, object?>?> QueryRowAsync(string sql,
        params (string Name, object Value)[] parameters)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        foreach (var (name, value) in parameters) cmd.Parameters.AddWithValue(name, value);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        var row = new Dictionary<string, object?>();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        }

        return row;
    }
}
