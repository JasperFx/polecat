using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public record TallyBumped(int Amount);

/// <summary>
///     Self-aggregating snapshot for the gh-526 coverage. Additive on purpose, so a stream folded once
///     and a stream folded in pieces produce different numbers rather than the same one by luck.
/// </summary>
public partial class Gh526Tally
{
    public Guid Id { get; set; }
    public int Total { get; set; }
    public int EventCount { get; set; }

    public void Apply(TallyBumped e)
    {
        Total += e.Amount;
        EventCount++;
    }
}

/// <summary>
///     gh-526 (wolverine#2053 / marten#4085): on a <b>single-tenanted</b> store, events whose
///     <c>tenant_id</c> values disagree must still fold into ONE async aggregate — #529.
/// </summary>
/// <remarks>
///     <para>
///         <b>Why this exists locally when the shared suite is already enrolled.</b>
///         <c>SingleTenantedEventSlicingCompliance</c> went in with wave 14 and its one fact
///         self-skips on Polecat, by the suite's own design: Polecat is QuickAppend-only, and the
///         quick-append metadata path normalizes disagreeing per-event tenant ids to the session
///         tenant before they reach storage, so the precondition cannot be built through the shared
///         append surface. The suite skips rather than passing vacuously — which is right, and which
///         also means the gh-526 fix had <em>no</em> assertion behind it in this repository. #529 asked
///         for exactly this: coverage that builds the precondition through the storage layer instead.
///     </para>
///     <para>
///         <b>So the mixed rows are written with SQL, below the layer that normalizes them.</b> That is
///         not a shortcut around the append API — it is the only way to reach the state the original
///         report described, whose rows were written by a client stamping appends inconsistently
///         against an older schema. The daemon then reads exactly what that store had.
///     </para>
///     <para>
///         <b>The precondition is asserted before the behaviour is.</b> If a future change to the
///         events table left the UPDATE matching nothing, every assertion below would still pass — the
///         stream would simply be single-tenanted and fold correctly for a reason with nothing to do
///         with gh-526. That is the vacuous green the shared suite skips to avoid, and it would be
///         worse here, because a local test cannot skip itself into visibility.
///     </para>
///     <para>
///         Async only. Live and inline aggregation fold these events correctly and always did; the
///         slicing happens in the daemon, which is why gh-526 was a daemon bug.
///     </para>
/// </remarks>
public class gh_526_single_tenanted_slicing : IAsyncLifetime
{
    private const string Schema = "gh526_single_tenanted";
    private const string OtherTenant = "some-other-tenant";

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => await TestSchema.DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
        => DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;

            // No conjoined event tenancy: the store stays single-tenanted, which is the whole
            // precondition under test.
            opts.Projections.Snapshot<Gh526Tally>(SnapshotLifecycle.Async);
        });

    [Fact]
    public async Task async_projection_folds_one_stream_despite_disagreeing_tenant_ids()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Gh526Tally>(streamId,
                new TallyBumped(1), new TallyBumped(2), new TallyBumped(4));
            await session.SaveChangesAsync(Token);
        }

        // Rewrite the tail of the stream to a different tenant, below the append path that would
        // have normalized it away.
        var rewritten = await RewriteTailTenantAsync(streamId);
        rewritten.ShouldBe(2);

        (await DistinctTenantCountAsync(streamId)).ShouldBe(2,
            "the mixed-tenancy precondition was not built, so anything below would pass for a reason "
            + "unrelated to gh-526");

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();
            await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(60));
        }

        await using var query = store.QuerySession();
        var tally = await query.LoadAsync<Gh526Tally>(streamId, Token);

        tally.ShouldNotBeNull();

        // The load-bearing assertion. A store slicing per tenant writes a document that saw only
        // whichever tenant group landed last, so both numbers come up short rather than wrong in
        // some exotic way -- 1/1 or 6/2 instead of 7/3.
        tally.EventCount.ShouldBe(3);
        tally.Total.ShouldBe(7);
    }

    /// <summary>
    ///     A rebuild reads the same rows through the same slicer, so it has to reach the same answer.
    /// </summary>
    /// <remarks>
    ///     Worth its own fact rather than folding into the one above: the catch-up path and the rebuild
    ///     path build their slices at different times, and gh-526 resolved the tenancy question from
    ///     the session rather than from the projection. A rebuild opens its own.
    /// </remarks>
    [Fact]
    public async Task a_rebuild_reaches_the_same_aggregate()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Gh526Tally>(streamId,
                new TallyBumped(3), new TallyBumped(5));
            await session.SaveChangesAsync(Token);
        }

        (await RewriteTailTenantAsync(streamId)).ShouldBe(1);
        (await DistinctTenantCountAsync(streamId)).ShouldBe(2);

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.RebuildProjectionAsync<Gh526Tally>(Token);
        }

        await using var query = store.QuerySession();
        var tally = await query.LoadAsync<Gh526Tally>(streamId, Token);

        tally.ShouldNotBeNull();
        tally.EventCount.ShouldBe(2);
        tally.Total.ShouldBe(8);
    }

    /// <summary>
    ///     Everything but the first event of the stream moves to another tenant. Returns the row count
    ///     so the caller can assert the precondition was actually built.
    /// </summary>
    private static async Task<int> RewriteTailTenantAsync(Guid streamId)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            update [{Schema}].[pc_events]
            set tenant_id = @tenant
            where stream_id = @stream
              and version > 1
            """;
        cmd.Parameters.AddWithValue("@tenant", OtherTenant);
        cmd.Parameters.AddWithValue("@stream", streamId);

        return await cmd.ExecuteNonQueryAsync(Token);
    }

    private static async Task<int> DistinctTenantCountAsync(Guid streamId)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"select count(distinct tenant_id) from [{Schema}].[pc_events] where stream_id = @stream";
        cmd.Parameters.AddWithValue("@stream", streamId);

        return (int)(await cmd.ExecuteScalarAsync(Token))!;
    }
}
