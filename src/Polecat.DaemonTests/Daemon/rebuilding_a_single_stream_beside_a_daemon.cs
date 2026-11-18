using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #1060 (marten#6071): <c>Advanced.RebuildSingleStreamAsync</c> for a document an asynchronous single-stream
///     projection writes. It goes through a daemon of this store in this process when one runs the shard, is
///     refused when the shard is live on another node, and otherwise rebuilds offline.
/// </summary>
public class rebuilding_a_single_stream_beside_a_daemon : IAsyncLifetime
{
    private const string Schema = "rebuild_single_stream_daemon";
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(30);

    private readonly List<DocumentStore> _stores = new();
    private DocumentStore theStore = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        theStore = aStore();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var store in _stores) store.Dispose();
        return ValueTask.CompletedTask;
    }

    // Two stores over one schema stand in for two nodes
    private DocumentStore aStore()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Projections.Add<RebuiltRosterProjection>(ProjectionLifecycle.Async);
        });

        _stores.Add(store);
        return store;
    }

    private async Task<Guid> aRosterAsync(params object[] events)
    {
        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<RebuiltRoster>(id, events);
        await session.SaveChangesAsync(Ct);
        return id;
    }

    private async Task corruptAsync(Guid id)
    {
        await using var session = theStore.LightweightSession();
        session.Store(new RebuiltRoster { Id = id, Members = 999 });
        await session.SaveChangesAsync(Ct);
    }

    private async Task<RebuiltRoster?> loadAsync(Guid id)
    {
        await using var query = theStore.QuerySession();
        return await query.LoadAsync<RebuiltRoster>(id, Ct);
    }

    [Fact]
    public async Task goes_through_the_daemon_that_runs_the_shard_in_this_process()
    {
        var id = await aRosterAsync(new RosterJoined(), new RosterJoined());

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await theStore.Database.WaitForNonStaleProjectionDataAsync(Generous);

        await corruptAsync(id);

        // The shard has just written its progression row, so anything but the daemon path would be refused
        await theStore.Advanced.RebuildSingleStreamAsync<RebuiltRoster>(id, Ct);

        (await loadAsync(id)).ShouldNotBeNull().Members.ShouldBe(2);

        await daemon.StopAllAsync();
    }

    [Fact]
    public async Task is_refused_when_the_shard_is_live_on_another_node()
    {
        var id = await aRosterAsync(new RosterJoined());

        // This store cannot reach the other node's daemon
        var otherNode = aStore();
        using var daemon = (IProjectionDaemon)await otherNode.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await otherNode.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var refused = await Should.ThrowAsync<IdentityRebuildRefusedException>(() =>
            theStore.Advanced.RebuildSingleStreamAsync<RebuiltRoster>(id, Ct));

        refused.Reason.ShouldBe(IdentityRebuildRefusal.ShardLiveOnAnotherNode);

        await daemon.StopAllAsync();
    }

    [Fact]
    public async Task rebuilds_offline_when_no_daemon_has_run_the_shard()
    {
        var id = await aRosterAsync(new RosterJoined(), new RosterJoined(), new RosterJoined());

        await theStore.Advanced.RebuildSingleStreamAsync<RebuiltRoster>(id, Ct);

        (await loadAsync(id)).ShouldNotBeNull().Members.ShouldBe(3);
    }

    [Fact]
    public async Task a_stream_that_folds_to_nothing_deletes_the_document()
    {
        // A document left behind for a stream that no longer folds to anything
        var id = await aRosterAsync(new RosterJoined(), new RosterDisbanded());
        await corruptAsync(id);

        await theStore.Advanced.RebuildSingleStreamAsync<RebuiltRoster>(id, Ct);

        (await loadAsync(id)).ShouldBeNull();
    }
}

public record RosterJoined;

public record RosterDisbanded;

public class RebuiltRoster
{
    public Guid Id { get; set; }
    public int Members { get; set; }
}

public partial class RebuiltRosterProjection : SingleStreamProjection<RebuiltRoster, Guid>
{
    public RebuiltRoster Create(RosterJoined _) => new() { Members = 1 };

    public void Apply(RosterJoined _, RebuiltRoster roster) => roster.Members++;

    public bool ShouldDelete(RosterDisbanded _) => true;
}
