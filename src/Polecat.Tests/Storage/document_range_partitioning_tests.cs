using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Storage;

public class MetricsSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public string Metric { get; set; } = string.Empty;
    public double Value { get; set; }
}

// Distinct type for the roll-forward test so its partition function (named from the table) does not
// collide with the other tests' — SQL Server partition functions/schemes are database-scoped.
public class RolledMetricsSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

// Distinct type so its database-scoped partition function/scheme don't collide with the others'.
public class ExternalMetricsSample
{
    public Guid Id { get; set; }
    public DateTimeOffset BucketEnd { get; set; }
    public double Value { get; set; }
}

[Collection("integration")]
public class document_range_partitioning_tests : IntegrationContext
{
    private const string Schema = "doc_partitioning";

    private static readonly DateTimeOffset Jan = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset Feb = new(2026, 2, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset Mar = new(2026, 3, 1, 0, 0, 0, TimeSpan.Zero);

    public document_range_partitioning_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<int> ScalarAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    // Drop the table and its (database-scoped) partition function/scheme so each test starts clean
    // regardless of state left by a prior run.
    private async Task ResetAsync(string table)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID('[{Schema}].[{table}]','U') IS NOT NULL DROP TABLE [{Schema}].[{table}];
            IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name='ps_{table}_bucket_end') DROP PARTITION SCHEME [ps_{table}_bucket_end];
            IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name='pf_{table}_bucket_end') DROP PARTITION FUNCTION [pf_{table}_bucket_end];
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    private Task<int> PartitionCountAsync(string table) => ScalarAsync(
        $"""
         SELECT COUNT(*) FROM sys.partitions p
         JOIN sys.objects o ON p.object_id = o.object_id
         JOIN sys.schemas s ON o.schema_id = s.schema_id
         WHERE s.name = '{Schema}' AND o.name = '{table}' AND p.index_id IN (0, 1)
         """);

    [Fact]
    public async Task creates_partition_function_scheme_and_partitions()
    {
        await ResetAsync("pc_doc_metricssample");
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<MetricsSample>().PartitionByRange(x => x.BucketEnd, Jan, Feb, Mar);
        });

        (await ScalarAsync(
            "SELECT COUNT(*) FROM sys.partition_functions WHERE name = 'pf_pc_doc_metricssample_bucket_end'"))
            .ShouldBe(1);
        (await ScalarAsync(
            "SELECT COUNT(*) FROM sys.partition_schemes WHERE name = 'ps_pc_doc_metricssample_bucket_end'"))
            .ShouldBe(1);

        // 3 boundaries -> 4 partitions.
        (await PartitionCountAsync("pc_doc_metricssample")).ShouldBe(4);

        // The promoted partition column is part of the primary key.
        (await ScalarAsync(
            $"""
             SELECT COUNT(*) FROM sys.index_columns ic
             JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
             WHERE i.is_primary_key = 1 AND c.name = 'bucket_end'
               AND ic.object_id = OBJECT_ID('[{Schema}].[pc_doc_metricssample]')
             """)).ShouldBe(1);
    }

    [Fact]
    public async Task stored_documents_populate_partition_column_and_land_in_distinct_partitions()
    {
        await ResetAsync("pc_doc_metricssample");
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<MetricsSample>().PartitionByRange(x => x.BucketEnd, Jan, Feb, Mar);
        });

        var janDoc = new MetricsSample { Id = Guid.NewGuid(), BucketEnd = Jan.AddDays(14), Metric = "cpu", Value = 1 };
        var febDoc = new MetricsSample { Id = Guid.NewGuid(), BucketEnd = Feb.AddDays(14), Metric = "cpu", Value = 2 };
        var marDoc = new MetricsSample { Id = Guid.NewGuid(), BucketEnd = Mar.AddDays(14), Metric = "cpu", Value = 3 };

        theSession.Store(janDoc, febDoc, marDoc);
        await theSession.SaveChangesAsync();

        // Documents round-trip through the JSON body unaffected by the duplicated column.
        var loaded = await theSession.LoadAsync<MetricsSample>(febDoc.Id);
        loaded.ShouldNotBeNull();
        loaded!.BucketEnd.ShouldBe(febDoc.BucketEnd);
        loaded.Value.ShouldBe(2);

        // The promoted column mirrors the document value.
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT bucket_end FROM [{Schema}].[pc_doc_metricssample] WHERE id = @id";
        var p = cmd.CreateParameter();
        p.ParameterName = "@id";
        p.Value = febDoc.Id;
        cmd.Parameters.Add(p);
        var storedBucket = (DateTimeOffset)(await cmd.ExecuteScalarAsync())!;
        storedBucket.ShouldBe(febDoc.BucketEnd);

        // The three documents land in three different physical partitions.
        (await ScalarAsync(
            $"""
             SELECT COUNT(DISTINCT $PARTITION.pf_pc_doc_metricssample_bucket_end(bucket_end))
             FROM [{Schema}].[pc_doc_metricssample]
             """)).ShouldBe(3);
    }

    [Fact]
    public async Task additive_boundary_rolls_a_new_partition_forward_without_losing_data()
    {
        await ResetAsync("pc_doc_rolledmetricssample");
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<RolledMetricsSample>().PartitionByRange(x => x.BucketEnd, Jan, Feb);
        });

        var doc = new RolledMetricsSample { Id = Guid.NewGuid(), BucketEnd = Jan.AddDays(10), Value = 7 };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        (await PartitionCountAsync("pc_doc_rolledmetricssample")).ShouldBe(3); // 2 boundaries -> 3 partitions

        // Roll March forward by re-applying the schema with an extra boundary.
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<RolledMetricsSample>().PartitionByRange(x => x.BucketEnd, Jan, Feb, Mar);
        });

        (await PartitionCountAsync("pc_doc_rolledmetricssample")).ShouldBe(4); // SPLIT RANGE added one partition

        // Pre-existing data survived the in-place split (no rebuild).
        var reloaded = await theSession.LoadAsync<RolledMetricsSample>(doc.Id);
        reloaded.ShouldNotBeNull();
        reloaded!.Value.ShouldBe(7);
    }

    [Fact]
    public async Task conjoined_tenancy_partitioning_is_rejected()
    {
        var ex = await Should.ThrowAsync<NotSupportedException>(async () =>
        {
            await StoreOptions(opts =>
            {
                opts.DatabaseSchemaName = "doc_partitioning_conjoined";
                opts.Events.TenancyStyle = TenancyStyle.Conjoined;
                opts.Schema.For<MetricsSample>().PartitionByRange(x => x.BucketEnd, Jan, Feb);
            });
        });

        ex.Message.ShouldContain("conjoined");
    }

    // ---- #255: Marten-parity PartitionOn(...).ByRange(...) DSL ----------------------------------

    [Fact]
    public async Task partition_on_by_range_is_equivalent_to_partition_by_range()
    {
        await ResetAsync("pc_doc_metricssample");
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            // The Marten-parity fluent form over the same machinery as PartitionByRange.
            opts.Schema.For<MetricsSample>().PartitionOn(x => x.BucketEnd).ByRange(Jan, Feb, Mar);
        });

        theSession.Store(new MetricsSample
        {
            Id = Guid.NewGuid(), BucketEnd = Jan.AddDays(5), Metric = "cpu", Value = 1
        });
        await theSession.SaveChangesAsync();

        (await PartitionCountAsync("pc_doc_metricssample")).ShouldBe(4); // 3 boundaries -> 4 partitions
    }

    // ---- #255: externally-managed range partitioning (time-series retention) --------------------

    [Fact]
    public async Task externally_managed_range_is_provisioned_once_and_never_reconciled()
    {
        const string table = "pc_doc_externalmetricssample";
        await ResetAsync(table);

        // Initial boundaries [Jan, Feb] -> 3 partitions. Externally-managed means Polecat creates the
        // partitioned table once and never reconciles it afterward.
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = Schema;
            opts.Schema.For<ExternalMetricsSample>()
                .PartitionOn(x => x.BucketEnd).ByExternallyManagedRange(Jan, Feb);
        });

        theSession.Store(new ExternalMetricsSample { Id = Guid.NewGuid(), BucketEnd = Jan.AddDays(3), Value = 1 });
        await theSession.SaveChangesAsync();

        (await PartitionCountAsync(table)).ShouldBe(3);

        // Simulate the app/DBA rolling March forward at runtime (the partition Polecat must not touch).
        await SplitRangeAsync(table, "2026-03-01T00:00:00.0000000+00:00");
        (await PartitionCountAsync(table)).ShouldBe(4);

        // A bulk schema apply must NOT reconcile the externally-managed table back to its declared
        // [Jan, Feb] boundaries — the app-managed March partition must survive.
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        (await PartitionCountAsync(table)).ShouldBe(4);

        // And the data is intact + still queryable.
        await using var query = theStore.QuerySession();
        (await query.Query<ExternalMetricsSample>().CountAsync()).ShouldBe(1);
    }

    private async Task SplitRangeAsync(string table, string boundaryLiteral)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            ALTER PARTITION SCHEME [ps_{table}_bucket_end] NEXT USED [PRIMARY];
            ALTER PARTITION FUNCTION [pf_{table}_bucket_end]() SPLIT RANGE (CONVERT(datetimeoffset, '{boundaryLiteral}'));
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
