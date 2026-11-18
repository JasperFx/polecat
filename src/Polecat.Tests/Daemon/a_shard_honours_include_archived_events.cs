using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #1065: the async daemon reads archived events for a projection that sets <c>IncludeArchivedEvents</c>, in a
///     continuous run and in a rebuild, as Marten's does. Before 6.0 the daemon's loader never passed the flag on,
///     so only the per-identity rebuild (#1060) read them, and the two could fold one stream differently.
/// </summary>
public class a_shard_honours_include_archived_events : IAsyncLifetime
{
    private const string Schema = "shard_include_archived";
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(30);

    private DocumentStore theStore = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);

        theStore = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Projections.Add<ArchivedLedgerProjection>(ProjectionLifecycle.Async);
            opts.Projections.Add<LiveLedgerProjection>(ProjectionLifecycle.Async);
        });

        // The 6.0 default, so the archived events sit in a partition of their own
        theStore.Options.EventGraph.UseArchivedStreamPartitioning.ShouldBeTrue();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    public ValueTask DisposeAsync()
    {
        theStore.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<Guid> anArchivedStreamAsync(int entries)
    {
        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, Enumerable.Range(1, entries).Select(x => (object)new LedgerEntry(x)).ToArray());
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(id);
            await session.SaveChangesAsync(Ct);
        }

        return id;
    }

    private async Task<T?> loadAsync<T>(Guid id) where T : notnull
    {
        await using var query = theStore.QuerySession();
        return await query.LoadAsync<T>(id, Ct);
    }

    [Fact]
    public async Task a_continuous_run_reads_archived_events_only_for_a_projection_that_asks_for_them()
    {
        var archived = await anArchivedStreamAsync(3);

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await theStore.Database.WaitForNonStaleProjectionDataAsync(Generous);

        (await loadAsync<ArchivedLedger>(archived)).ShouldNotBeNull().Total.ShouldBe(6);
        (await loadAsync<LiveLedger>(archived)).ShouldBeNull();

        await daemon.StopAllAsync();
    }

    [Fact]
    public async Task a_rebuild_reads_archived_events_only_for_a_projection_that_asks_for_them()
    {
        var archived = await anArchivedStreamAsync(4);

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.RebuildProjectionAsync("ArchivedLedger", Generous, Ct);
        await daemon.RebuildProjectionAsync("LiveLedger", Generous, Ct);

        (await loadAsync<ArchivedLedger>(archived)).ShouldNotBeNull().Total.ShouldBe(10);
        (await loadAsync<LiveLedger>(archived)).ShouldBeNull();
    }

    [Fact]
    public async Task the_shard_and_a_single_stream_rebuild_fold_an_archived_stream_to_the_same_document()
    {
        var archived = await anArchivedStreamAsync(2);

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await theStore.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var fromTheShard = (await loadAsync<ArchivedLedger>(archived)).ShouldNotBeNull();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new ArchivedLedger { Id = archived, Total = -1, Entries = 99 });
            await session.SaveChangesAsync(Ct);
        }

        // Through the daemon running the shard, with the per-identity rebuild's own loader
        await theStore.Advanced.RebuildSingleStreamAsync<ArchivedLedger>(archived, Ct);

        var fromTheRebuild = (await loadAsync<ArchivedLedger>(archived)).ShouldNotBeNull();
        fromTheRebuild.Total.ShouldBe(fromTheShard.Total);
        fromTheRebuild.Entries.ShouldBe(fromTheShard.Entries);
        fromTheRebuild.Total.ShouldBe(3);

        await daemon.StopAllAsync();
    }
}

public record LedgerEntry(int Amount);

public class ArchivedLedger
{
    public Guid Id { get; set; }
    public int Total { get; set; }
    public int Entries { get; set; }
}

public class LiveLedger
{
    public Guid Id { get; set; }
    public int Total { get; set; }
}

public partial class ArchivedLedgerProjection : SingleStreamProjection<ArchivedLedger, Guid>
{
    public ArchivedLedgerProjection()
    {
        Name = "ArchivedLedger";
        IncludeArchivedEvents = true;
    }

    public ArchivedLedger Create(LedgerEntry e) => new() { Total = e.Amount, Entries = 1 };

    public void Apply(LedgerEntry e, ArchivedLedger ledger)
    {
        ledger.Total += e.Amount;
        ledger.Entries++;
    }
}

public partial class LiveLedgerProjection : SingleStreamProjection<LiveLedger, Guid>
{
    public LiveLedgerProjection()
    {
        Name = "LiveLedger";
    }

    public LiveLedger Create(LedgerEntry e) => new() { Total = e.Amount };

    public void Apply(LedgerEntry e, LiveLedger ledger) => ledger.Total += e.Amount;
}
