using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Internal.Operations;
using Polecat.Linq.SqlGeneration;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>
///     #834 under <c>UseTenantPartitionedEvents</c>, which is the one mode where an event binds a ninth
///     parameter, where <c>seq_id</c> comes from a per-tenant sequence, and where a delete keyed on
///     <c>seq_id</c> would reach into every other tenant without its tenant predicate (marten#5234).
/// </summary>
[Collection("tenant-partitioning")]
public class parameter_limit_under_tenant_partitioning_tests : IAsyncLifetime
{
    private const string Schema = "gh834_partitioned";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await ExecuteAsync($"""
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP TABLE [{Schema}].[' + t.name + '];'
            FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '{Schema}';
            IF @sql <> N'' EXEC sp_executesql @sql;
            """);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await ExecuteAsync($"""
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP SEQUENCE [{Schema}].[' + o.name + '];'
            FROM sys.sequences o JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE s.name = '{Schema}';
            IF @sql <> N'' EXEC sp_executesql @sql;
            """);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static DocumentStore CreateStore(Action<StoreOptions>? configure = null)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            configure?.Invoke(opts);
        });
    }

    private static object[] Quest(int count, string name)
    {
        var events = new object[count];
        events[0] = new QuestStarted(name);
        for (var i = 1; i < count; i++)
        {
            events[i] = new MonsterSlain($"{name}-{i}", i);
        }

        return events;
    }

    [Fact]
    public async Task one_save_appends_past_the_limit_and_reads_its_sequences_back()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" });
        var stream = session.Events.StartStream(id, Quest(5000, "alpha"));
        await session.SaveChangesAsync(Ct);

        // The sequence is this tenant's own, so a fresh tenant's stream starts at 1 and runs unbroken.
        stream.Events.Select(x => x.Sequence).ShouldBe(Enumerable.Range(1, 5000).Select(x => (long)x));
        stream.Events.Select(x => x.Version).ShouldBe(Enumerable.Range(1, 5000).Select(x => (long)x));

        await using var query = store.QuerySession(new SessionOptions { TenantId = "alpha" });
        var stored = await query.Events.FetchStreamAsync(id, token: Ct);
        stored.Select(x => (x.Id, x.Sequence)).ShouldBe(stream.Events.Select(x => (x.Id, x.Sequence)));

        var state = await query.Events.FetchStreamStateAsync(id, Ct);
        state!.Version.ShouldBe(5000);
    }

    /// <summary>
    ///     Both tenants number from 1, so the delete names every sequence beta has too. Only the
    ///     predicate stands between one tenant's compaction and the other's data.
    /// </summary>
    [Fact]
    public async Task compaction_past_the_limit_leaves_the_other_tenant_alone()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var alpha = Guid.NewGuid();
        var beta = Guid.NewGuid();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Events.StartStream(alpha, Quest(5500, "alpha"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "beta" }))
        {
            session.Events.StartStream(beta, Quest(2500, "beta"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            await session.Events.CompactStreamAsync<QuestParty>(alpha);
            await session.SaveChangesAsync(Ct);
        }

        await using var alphaQuery = store.QuerySession(new SessionOptions { TenantId = "alpha" });
        var compacted = (await alphaQuery.Events.FetchStreamAsync(alpha, token: Ct)).Single();
        compacted.Data.ShouldBeOfType<Compacted<QuestParty>>().Snapshot.MonstersSlain.Count.ShouldBe(5499);

        await using var betaQuery = store.QuerySession(new SessionOptions { TenantId = "beta" });
        var untouched = await betaQuery.Events.FetchStreamAsync(beta, token: Ct);

        untouched.Count.ShouldBe(2500);
        untouched.ShouldAllBe(x => !(x.Data is Compacted<QuestParty>));
    }

    /// <summary>See the matching test in <c>appending_past_the_parameter_limit_tests</c>.</summary>
    [Fact]
    public async Task every_command_fills_the_budget_and_none_passes_it()
    {
        using var store = CreateStore(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
            opts.Events.EnableHeaders = true;
            opts.Events.EnableUserName = true;
        });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var id = Guid.NewGuid();

        // Once for the new stream's INSERT and once for the existing stream's UPDATE.
        for (var pass = 0; pass < 2; pass++)
        {
            await using var session = (DocumentSessionBase)store.LightweightSession(
                new SessionOptions { TenantId = "alpha" });

            session.Events.Append(id, Enumerable.Range(0, 1200)
                .Select(i => (object)new MonsterSlain($"monster-{i}", i)).ToArray());

            var operations = await session.BuildRaisedEventAppendsAsync(session.WorkTracker.Streams.ToList(), Ct);
            operations.Count.ShouldBeGreaterThan(1);

            await using var batch = new SqlBatch();
            var builder = new BatchBuilder(batch);
            for (var i = 0; i < operations.Count; i++)
            {
                if (i > 0) builder.StartNewCommand();
                StorageOperationExecution.Configure(operations[i], builder, session);
            }

            builder.Compile();

            var counts = batch.BatchCommands.Cast<SqlBatchCommand>().Select(x => x.Parameters.Count).ToArray();

            counts.ShouldAllBe(x => x <= JsonValueList.Budget);

            // An event here costs 13 parameters.
            counts.SkipLast(1).ShouldAllBe(x => x > JsonValueList.Budget - 13);

            if (pass == 0)
            {
                await using var starter = store.LightweightSession(new SessionOptions { TenantId = "alpha" });
                starter.Events.StartStream(id, new MonsterSlain("first", 1));
                await starter.SaveChangesAsync(Ct);
            }
        }
    }
}
