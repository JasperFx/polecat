using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #916, the #363 mistake on the progression table. <c>name</c> is <c>varchar</c>, and a shard's
///     name bound as <c>nvarchar</c> converts the column rather than the parameter, so the write walks
///     the whole table and takes an update lock on every row it passes. The table is small, so that was
///     nothing to read, but each of those is a row some other shard's open batch may hold.
/// </summary>
public class progression_write_binding_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // Plenty for a write that takes no notice of the open one. Anything blocked by it never finishes.
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    private async Task SeedShardsAsync(params string[] names)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        foreach (var name in names)
        {
            await WriteProgressAsync(name, 1, upsert: true);
        }
    }

    private async Task WriteProgressAsync(string name, long ceiling, bool upsert)
    {
        var builder = new BatchBuilder();
        StorageOperationExecution.Configure(
            theStore.Events.UpdateProgressOperation(name, ceiling, upsert), builder, session: null);
        await using var batch = builder.Compile();

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        batch.Connection = conn;
        await batch.ExecuteNonQueryAsync(Ct);
    }

    private async Task<long> ProgressOfAsync(string name)
    {
        var rows = await TestSchema.QueryAsync(
            $"SELECT last_seq_id FROM {theStore.Events.ProgressionTableName} WHERE name = '{name}'");
        return (long)rows.Single()[0];
    }

    /// <summary>What a batch looks like from outside between its progression row and its commit.</summary>
    private async Task<(SqlConnection, SqlTransaction)> OpenBatchOnAsync(string name)
    {
        var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        var transaction = conn.BeginTransaction();

        await using var cmd = conn.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = $"UPDATE {theStore.Events.ProgressionTableName} SET last_seq_id = 2 WHERE name = '{name}'";
        await cmd.ExecuteNonQueryAsync(Ct);

        return (conn, transaction);
    }

    [Fact]
    public async Task one_shards_open_batch_does_not_hold_up_anothers_progress()
    {
        await SeedShardsAsync("A:All", "B:All");

        var (conn, transaction) = await OpenBatchOnAsync("A:All");
        await using (conn)
        await using (transaction)
        {
            await WriteProgressAsync("B:All", 5, upsert: false).WaitAsync(Bound, Ct);
        }

        (await ProgressOfAsync("B:All")).ShouldBe(5);
    }

    [Fact]
    public async Task one_shards_open_batch_does_not_hold_up_a_new_row_for_another()
    {
        await SeedShardsAsync("A:All", "Z:All");

        var (conn, transaction) = await OpenBatchOnAsync("A:All");
        await using (conn)
        await using (transaction)
        {
            await WriteProgressAsync("M:All", 5, upsert: true).WaitAsync(Bound, Ct);
        }

        (await ProgressOfAsync("M:All")).ShouldBe(5);
    }

    [Fact]
    public async Task the_upsert_still_updates_a_row_that_is_there()
    {
        await SeedShardsAsync("A:All");

        await WriteProgressAsync("A:All", 9, upsert: true);

        (await ProgressOfAsync("A:All")).ShouldBe(9);
    }
}
