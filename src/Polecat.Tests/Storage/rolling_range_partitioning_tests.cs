using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core.Partitioning;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Tests.Storage;

/// <summary>
///     #386: rolling time-window RANGE partitions for time-series document tables. The window is a pure
///     function of the policy and the clock, so every test here drives a <see cref="MutableTimeProvider" />
///     rather than waiting on the calendar.
/// </summary>
[Collection("integration")]
public class rolling_range_partitioning_tests : IntegrationContext
{
    private const string Schema = "doc_rolling_partitioning";

    // Mid-month on purpose: nothing here may depend on "now" landing on a period boundary.
    private static readonly DateTimeOffset July = new(2026, 7, 15, 9, 30, 0, TimeSpan.Zero);

    public rolling_range_partitioning_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // ---- window shape -------------------------------------------------------------------------

    [Fact]
    public async Task creates_the_whole_declared_window_on_first_migration()
    {
        const string table = "pc_doc_rollingmetricssample";
        await ResetAsync(table);

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<RollingMetricsSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1,
                    new MutableTimeProvider(July));
        });

        (await ScalarAsync($"SELECT COUNT(*) FROM sys.partition_functions WHERE name = 'pf_{table}_bucket_end'"))
            .ShouldBe(1);
        (await ScalarAsync($"SELECT COUNT(*) FROM sys.partition_schemes WHERE name = 'ps_{table}_bucket_end'"))
            .ShouldBe(1);

        // June, July, August — the three periods of a (1 ahead, 1 behind) monthly window — plus the
        // exclusive end of the newest provisioned period, which keeps the top partition empty so every
        // later SPLIT stays metadata-only.
        (await BoundaryCountAsync(table)).ShouldBe(4);
        (await PartitionCountAsync(table)).ShouldBe(5);

        // The promoted partition column joins the primary key, as SQL Server requires.
        (await ScalarAsync(
            $"""
             SELECT COUNT(*) FROM sys.index_columns ic
             JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
             WHERE i.is_primary_key = 1 AND c.name = 'bucket_end'
               AND ic.object_id = OBJECT_ID('[{Schema}].[{table}]')
             """)).ShouldBe(1);
    }

    [Fact]
    public async Task documents_land_in_the_partition_for_their_period()
    {
        const string table = "pc_doc_rollingmetricssample";
        await ResetAsync(table);

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<RollingMetricsSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1,
                    new MutableTimeProvider(July));
        });

        var june = new RollingMetricsSample { Id = Guid.NewGuid(), BucketEnd = July.AddMonths(-1), Value = 1 };
        var july = new RollingMetricsSample { Id = Guid.NewGuid(), BucketEnd = July, Value = 2 };
        var august = new RollingMetricsSample { Id = Guid.NewGuid(), BucketEnd = July.AddMonths(1), Value = 3 };

        theSession.Store(june, july, august);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await theSession.LoadAsync<RollingMetricsSample>(july.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.BucketEnd.ShouldBe(july.BucketEnd);

        // Three periods, three physical partitions.
        (await ScalarAsync(
            $"""
             SELECT COUNT(DISTINCT $PARTITION.pf_{table}_bucket_end(bucket_end))
             FROM [{Schema}].[{table}]
             """)).ShouldBe(3);
    }

    [Fact]
    public async Task a_row_outside_the_provisioned_window_is_stored_rather_than_rejected()
    {
        const string table = "pc_doc_overflowmetricssample";
        await ResetAsync(table);

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<OverflowMetricsSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1,
                    new MutableTimeProvider(July));
        });

        // A SQL Server RANGE function always spans (-infinity, +infinity), so the outermost partitions
        // absorb anything outside the window — there is no PostgreSQL-style "no partition of relation"
        // rejection to guard against, and no DEFAULT partition to declare.
        var ancient = new OverflowMetricsSample
        {
            Id = Guid.NewGuid(), BucketEnd = July.AddYears(-5), Value = 1
        };

        theSession.Store(ancient);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await theSession.LoadAsync<OverflowMetricsSample>(ancient.Id, TestContext.Current.CancellationToken))
            .ShouldNotBeNull();
    }

    // ---- roll-forward -------------------------------------------------------------------------

    [Fact]
    public async Task rolling_the_window_forward_is_additive_and_never_a_rebuild()
    {
        const string table = "pc_doc_rolledwindowsample";
        await ResetAsync(table);

        var clock = new MutableTimeProvider(July);
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<RolledWindowSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1, clock);
        });

        var doc = new RolledWindowSample { Id = Guid.NewGuid(), BucketEnd = July, Value = 7 };
        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await BoundaryCountAsync(table)).ShouldBe(4);

        // One month later the declared window is [Jul, Aug, Sep] against a database holding
        // [Jun, Jul, Aug]: the June boundary the declaration no longer names is an aged period, not
        // drift, so ordinary schema migration only ever SPLITs the new leading edge in.
        clock.UtcNow = July.AddMonths(1);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        (await BoundaryCountAsync(table)).ShouldBe(5);
        (await PartitionCountAsync(table)).ShouldBe(6);

        // A rebuild would have taken the table with it. The row is the proof it was a SPLIT.
        var reloaded = await theSession.LoadAsync<RolledWindowSample>(doc.Id, TestContext.Current.CancellationToken);
        reloaded.ShouldNotBeNull();
        reloaded!.Value.ShouldBe(7);
    }

    [Fact]
    public async Task one_shared_manager_rolls_every_table_wired_to_it_forward()
    {
        await ResetAsync("pc_doc_sharedwindowa");
        await ResetAsync("pc_doc_sharedwindowb");

        var clock = new MutableTimeProvider(July);
        var manager = new ManagedRangePartitions(
            RollingWindowPolicy.Monthly(periodsAhead: 1, periodsBehind: 1),
            column: "bucket_end", sqlDataType: "datetimeoffset", clock);

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<SharedWindowA>().PartitionOn(x => x.BucketEnd).ByRollingRange(manager);
            opts.Schema.For<SharedWindowB>().PartitionOn(x => x.BucketEnd).ByRollingRange(manager);
        });

        (await BoundaryCountAsync("pc_doc_sharedwindowa")).ShouldBe(4);
        (await BoundaryCountAsync("pc_doc_sharedwindowb")).ShouldBe(4);

        // One pass over one manager, both tables rolled forward.
        clock.UtcNow = July.AddMonths(1);
        var statuses = await theStore.Advanced.RollPartitionsForwardAsync(TestContext.Current.CancellationToken);

        statuses.ShouldAllBe(x => x.Status == PartitionMigrationStatus.Complete);
        (await BoundaryCountAsync("pc_doc_sharedwindowa")).ShouldBe(5);
        (await BoundaryCountAsync("pc_doc_sharedwindowb")).ShouldBe(5);
    }

    // ---- retention ----------------------------------------------------------------------------

    [Fact]
    public async Task the_additive_and_retention_halves_can_be_run_separately()
    {
        const string table = "pc_doc_retainedwindowsample";
        await ResetAsync(table);

        var clock = new MutableTimeProvider(July);
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<RetainedWindowSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1, clock);
        });

        var june = new RetainedWindowSample { Id = Guid.NewGuid(), BucketEnd = July.AddMonths(-1), Value = 1 };
        var july = new RetainedWindowSample { Id = Guid.NewGuid(), BucketEnd = July, Value = 2 };
        var august = new RetainedWindowSample { Id = Guid.NewGuid(), BucketEnd = July.AddMonths(1), Value = 3 };

        theSession.Store(june, july, august);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Two months on, the retention floor is August: June and July have aged out.
        clock.UtcNow = July.AddMonths(2);

        // The additive half provisions October and November and touches nothing else — in particular it
        // makes no retention decision, so all three rows are still there.
        await theStore.Advanced.RollPartitionsForwardAsync(TestContext.Current.CancellationToken);
        (await BoundaryCountAsync(table)).ShouldBe(6);
        await CountShouldBeAsync<RetainedWindowSample>(3);

        // The retention half truncates the aged partitions and merges their boundaries away, leaving the
        // window exactly as declared.
        await theStore.Advanced.DropAgedRollingPartitionsAsync(TestContext.Current.CancellationToken);
        (await BoundaryCountAsync(table)).ShouldBe(4);
        (await PartitionCountAsync(table)).ShouldBe(5);

        await CountShouldBeAsync<RetainedWindowSample>(1);
        await using var query = theStore.QuerySession();
        var survivor = await query.Query<RetainedWindowSample>()
            .SingleAsync(TestContext.Current.CancellationToken);
        survivor.Id.ShouldBe(august.Id);
    }

    [Fact]
    public async Task the_apply_pass_is_idempotent()
    {
        const string table = "pc_doc_idempotentwindowsample";
        await ResetAsync(table);

        var clock = new MutableTimeProvider(July);
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<IdempotentWindowSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1, clock);
        });

        theSession.Store(new IdempotentWindowSample
        {
            Id = Guid.NewGuid(), BucketEnd = July.AddMonths(1), Value = 1
        });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        clock.UtcNow = July.AddMonths(2);

        for (var i = 0; i < 3; i++)
        {
            var statuses = await theStore.Advanced
                .ApplyRollingPartitionsAsync(TestContext.Current.CancellationToken);

            statuses.ShouldNotBeEmpty();
            statuses.ShouldAllBe(x => x.Status == PartitionMigrationStatus.Complete);

            (await BoundaryCountAsync(table)).ShouldBe(4);
            (await PartitionCountAsync(table)).ShouldBe(5);
        }

        // The August row is inside the retained window and survives every pass.
        await CountShouldBeAsync<IdempotentWindowSample>(1);
    }

    [Fact]
    public async Task concurrent_apply_passes_do_not_throw_and_converge_on_the_window()
    {
        const string table = "pc_doc_concurrentwindowsample";
        await ResetAsync(table);

        var clock = new MutableTimeProvider(July);
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<ConcurrentWindowSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1, clock);
        });

        clock.UtcNow = July.AddMonths(2);

        // Several nodes starting at once. A losing SPLIT/MERGE is reported per table rather than thrown,
        // and the window still ends up exactly as declared.
        await Task.WhenAll(Enumerable.Range(0, 3).Select(_ =>
            theStore.Advanced.ApplyRollingPartitionsAsync(TestContext.Current.CancellationToken)));

        (await BoundaryCountAsync(table)).ShouldBe(4);
        (await PartitionCountAsync(table)).ShouldBe(5);
    }

    // ---- host startup ------------------------------------------------------------------------

    [Fact]
    public async Task host_startup_rolls_forward_and_retires_under_ApplyAllDatabaseChangesOnStartup()
    {
        const string table = "pc_doc_startupwindowsample";
        await ResetAsync(table);

        // Provision the window as it stood in July, with a row in a period that will later age out.
        var aged = Guid.NewGuid();
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<StartupWindowSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1,
                    new MutableTimeProvider(July));
        });

        theSession.Store(new StartupWindowSample { Id = aged, BucketEnd = July.AddMonths(-1), Value = 1 });
        theSession.Store(new StartupWindowSample
        {
            Id = Guid.NewGuid(), BucketEnd = July.AddMonths(1), Value = 2
        });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Two months later the same application starts again. Nothing but the startup opt-in drives the
        // roll-forward and the retirement — no application-authored DDL anywhere.
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Schema.For<StartupWindowSample>()
                .PartitionOn(x => x.BucketEnd)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1,
                    new MutableTimeProvider(July.AddMonths(2)));
        }).ApplyAllDatabaseChangesOnStartup();

        await using var provider = services.BuildServiceProvider();
        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(TestContext.Current.CancellationToken);
        }

        (await BoundaryCountAsync(table)).ShouldBe(4);
        (await PartitionCountAsync(table)).ShouldBe(5);

        var store = provider.GetRequiredService<IDocumentStore>();
        await using var query = store.QuerySession();
        var remaining = await query.Query<StartupWindowSample>().ToListAsync(TestContext.Current.CancellationToken);
        remaining.Count.ShouldBe(1);
        remaining[0].Id.ShouldNotBe(aged);
    }

    // ---- configuration-time guards -----------------------------------------------------------

    [Fact]
    public void rejects_a_non_temporal_partition_key()
    {
        var options = new StoreOptions();

        var ex = Should.Throw<InvalidOperationException>(() =>
            options.Schema.For<NonTemporalSample>()
                .PartitionOn(x => x.Sequence)
                .ByRollingRange(PartitionPeriod.Month, periodsAhead: 1, periodsBehind: 1));

        ex.Message.ShouldContain("DateTime or DateTimeOffset");
        ex.Message.ShouldContain("sequence");
    }

    [Fact]
    public void rejects_a_shared_manager_whose_column_does_not_match_the_member()
    {
        var options = new StoreOptions();
        var manager = new ManagedRangePartitions(
            RollingWindowPolicy.Monthly(periodsAhead: 1, periodsBehind: 1), column: "occurred_at");

        var ex = Should.Throw<InvalidOperationException>(() =>
            options.Schema.For<RollingMetricsSample>().PartitionOn(x => x.BucketEnd).ByRollingRange(manager));

        ex.Message.ShouldContain("occurred_at");
        ex.Message.ShouldContain("bucket_end");
    }

    // ---- helpers -----------------------------------------------------------------------------

    private async Task CountShouldBeAsync<T>(int expected) where T : class
    {
        await using var query = theStore.QuerySession();
        (await query.Query<T>().CountAsync(TestContext.Current.CancellationToken)).ShouldBe(expected);
    }

    private async Task<int> ScalarAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private Task<int> BoundaryCountAsync(string table) => ScalarAsync(
        $"""
         SELECT COUNT(*) FROM sys.partition_range_values prv
         JOIN sys.partition_functions pf ON pf.function_id = prv.function_id
         WHERE pf.name = 'pf_{table}_bucket_end'
         """);

    private Task<int> PartitionCountAsync(string table) => ScalarAsync(
        $"""
         SELECT COUNT(*) FROM sys.partitions p
         JOIN sys.objects o ON p.object_id = o.object_id
         JOIN sys.schemas s ON o.schema_id = s.schema_id
         WHERE s.name = '{Schema}' AND o.name = '{table}' AND p.index_id IN (0, 1)
         """);

    /// <summary>
    ///     Drop the table and its database-scoped partition function/scheme so each test starts from
    ///     nothing regardless of what a prior run left behind.
    /// </summary>
    private async Task ResetAsync(string table)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID('[{Schema}].[{table}]','U') IS NOT NULL DROP TABLE [{Schema}].[{table}];
            IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name='ps_{table}_bucket_end') DROP PARTITION SCHEME [ps_{table}_bucket_end];
            IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name='pf_{table}_bucket_end') DROP PARTITION FUNCTION [pf_{table}_bucket_end];
            """;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}

/// <summary>
///     The only thing <see cref="ManagedRangePartitions" /> asks of a clock is "what time is it", so a
///     three-line provider beats taking on Microsoft.Extensions.TimeProvider.Testing for it.
/// </summary>
internal sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
{
    public DateTimeOffset UtcNow { get; set; } = now;

    public override DateTimeOffset GetUtcNow() => UtcNow;
}

// One document type per test: a SQL Server partition function and scheme are database-scoped objects
// named from the table, so sharing a type across tests would make them contend for the same objects.
public class RollingMetricsSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class OverflowMetricsSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class RolledWindowSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class RetainedWindowSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class IdempotentWindowSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class ConcurrentWindowSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class StartupWindowSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class SharedWindowA
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class SharedWindowB
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

public class NonTemporalSample
{
    public Guid Id { get; set; }
    public int Sequence { get; set; }
}
