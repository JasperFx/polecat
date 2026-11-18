using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     polecat#677 (porting marten#5512): every diagnostic read of the event-store tables answers
///     "no results" — empty, <c>null</c> or <c>0</c> — rather than throwing when the storage it reads
///     is not there. These are the methods CritterWatch polls per database, once per interval, for
///     every registered store; one of them throwing aborts the whole fan-out and the operator gets an
///     empty page for shards that do have data.
/// </summary>
/// <remarks>
///     Both failure modes are covered, and the second is the one that shipped a production failure in
///     Marten (marten#5509). <b>208</b> is a missing table, reached under <c>AutoCreate.None</c> on a
///     fresh database: being in the migration set does not help if nothing ever applies it.
///     <b>207</b> is a missing COLUMN, reached on a <c>pc_event_progression</c> created before
///     <c>EnableExtendedProgressionTracking</c> was migrated in. A guard handling only 208 looks
///     complete and misses it — which is why the drifted table here is built deliberately, in its old
///     shape, rather than simulated by turning the feature flag off (that would test nothing).
/// </remarks>
public class diagnostic_reads_tolerate_missing_storage_tests : OneOffConfigurationsContext
{
    private static readonly ShardName TheShard = new("Missing");

    [Fact]
    public async Task every_diagnostic_read_answers_nothing_on_a_schema_that_was_never_applied()
    {
        // The harness drops the schema in InitializeAsync, and AutoCreate.None means nothing will
        // recreate it — so every table these reads name is absent.
        ConfigureStore(opts => opts.AutoCreateSchemaObjects = AutoCreate.None);

        var token = TestContext.Current.CancellationToken;

        (await theDatabase.ProjectionProgressFor(TheShard, token)).ShouldBe(0);
        (await theDatabase.AllProjectionProgress(token)).ShouldBeEmpty();
        (await theDatabase.AllProjectionProgress("tenant1", token)).ShouldBeEmpty();
        (await theDatabase.ReadProjectionProgressAsync("Missing", null, token)).ShouldBeNull();
        (await theDatabase.ReadProjectionProgressAsync(TheShard, token)).ShouldBeNull();
        (await theDatabase.FetchHighestEventSequenceNumber(token)).ShouldBe(0);
        (await theDatabase.FindEventStoreFloorAtTimeAsync(DateTimeOffset.UtcNow.AddDays(-1), token))
            .ShouldBeNull();
        (await theDatabase.CountDeadLetterEventsAsync(TheShard, token)).ShouldBe(0);
        (await theDatabase.FetchDeadLetterCountsAsync(token)).ShouldBeEmpty();
        (await theDatabase.FetchDeadLetterCountsAsync("tenant1", token)).ShouldBeEmpty();
        (await theDatabase.QueryDeadLetterEventsAsync(TheShard, null, 0, 10, token)).ShouldBeEmpty();

        var statistics = await theStore.Advanced.FetchEventStoreStatistics(token);
        statistics.EventCount.ShouldBe(0);
        statistics.StreamCount.ShouldBe(0);
        statistics.EventSequenceNumber.ShouldBe(0);
    }

    [Fact]
    public async Task ejecting_a_shard_is_a_clean_no_op_when_the_progression_table_is_missing()
    {
        ConfigureStore(opts => opts.AutoCreateSchemaObjects = AutoCreate.None);

        // The one write inside the guard, because its own contract is already a no-op for a row that
        // is not there — a whole missing table is the same answer for the same reason.
        await theDatabase.DeleteProjectionProgressByShardNameAsync(TheShard.Identity,
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task a_progression_write_still_throws_when_the_storage_is_missing()
    {
        ConfigureStore(opts => opts.AutoCreateSchemaObjects = AutoCreate.None);

        // Writes are deliberately outside the guard: silently dropping a progression or telemetry
        // write is a worse failure than a read returning nothing.
        await Should.ThrowAsync<SqlException>(() =>
            theDatabase.WriteExtendedProgressionAsync(
                new ShardState(TheShard, 5L), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task progression_reads_answer_on_a_table_missing_the_extended_tracking_columns()
    {
        // #5509's exact shape: extended tracking is ON, so the reads select heartbeat, agent_status,
        // pause_reason and friends — against a pc_event_progression created before any of them
        // existed. AutoCreate.None keeps the migration from quietly fixing the table under us.
        ConfigureStore(opts =>
        {
            opts.Events.EnableExtendedProgressionTracking = true;
            opts.AutoCreateSchemaObjects = AutoCreate.None;
        });

        await CreateLegacyProgressionTableAsync();

        var token = TestContext.Current.CancellationToken;

        // 207, not 208 — the table is right there, and the columns are not.
        (await theDatabase.AllProjectionProgress(token)).ShouldBeEmpty();
        (await theDatabase.AllProjectionProgress("tenant1", token)).ShouldBeEmpty();
        (await theDatabase.ReadProjectionProgressAsync("Missing", null, token)).ShouldBeNull();
        (await theDatabase.ReadProjectionProgressAsync(TheShard, token)).ShouldBeNull();

        // This one only ever selects last_seq_id, so it reads the legacy table successfully and
        // reports the honest "no row" answer rather than being rescued by the guard.
        (await theDatabase.ProjectionProgressFor(TheShard, token)).ShouldBe(0);
    }

    [Fact]
    public async Task a_provisioned_event_store_still_reports_its_real_rows()
    {
        // The other direction, and the one a too-broad catch would break: storage present means the
        // real numbers, never "nothing".
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new ThingHappened("one"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var token = TestContext.Current.CancellationToken;

        (await theDatabase.FetchHighestEventSequenceNumber(token)).ShouldBeGreaterThan(0);
        (await theDatabase.FindEventStoreFloorAtTimeAsync(DateTimeOffset.UtcNow.AddDays(-1), token))
            .ShouldNotBeNull();

        var statistics = await theStore.Advanced.FetchEventStoreStatistics(token);
        statistics.EventCount.ShouldBe(1);
        statistics.StreamCount.ShouldBe(1);
    }

    [Fact]
    public async Task the_classifier_recognizes_a_real_invalid_column_error()
    {
        // 207 provoked from SQL Server rather than asserted about a hand-built exception, matching
        // how sqlserver_storage_dialect_tests pins 208.
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT no_such_column FROM {theStore.Options.SchemaResolver.ForEventProgression()};";

        var ex = await Should.ThrowAsync<SqlException>(() =>
            cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        ex.Number.ShouldBe(MissingStorageDetection.InvalidColumnName);
        MissingStorageDetection.IsMissingStorage(ex).ShouldBeTrue();

        // A missing column is NOT a missing table, so the narrower classifier the storage dialect
        // publishes must keep saying no.
        MissingStorageDetection.IsUndefinedTable(ex).ShouldBeFalse();

        // And nothing unrelated is swallowed.
        MissingStorageDetection.IsMissingStorage(new InvalidOperationException("nope")).ShouldBeFalse();
    }

    private async Task CreateLegacyProgressionTableAsync()
    {
        // Options.Events.DatabaseSchemaName is the nullable OVERRIDE; EventGraph resolves the
        // effective name, which is what the progression table is actually created in.
        var schema = theStore.Options.EventGraph.DatabaseSchemaName;
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]');
            CREATE TABLE [{schema}].[pc_event_progression] (
                name varchar(200) NOT NULL PRIMARY KEY,
                last_seq_id bigint NOT NULL DEFAULT 0,
                last_updated datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET()
            );
            """;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public record ThingHappened(string Name);
}
