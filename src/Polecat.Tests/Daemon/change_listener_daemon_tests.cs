using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Services;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
/// #269: post-commit change notifications for the async daemon. Ported from Marten's
/// DaemonTests/Internals/basic_functionality.cs (can_listen_for_commits_in_daemon,
/// listeners_are_not_active_in_rebuilds). Registered via Projections.AsyncListeners, an
/// IChangeListener fires BeforeCommitAsync / AfterCommitAsync around each daemon projection batch,
/// carrying an IChangeSet of the projected documents that changed.
/// </summary>
public class change_listener_daemon_tests : OneOffConfigurationsContext
{
    private readonly FakeChangeListener _listener = new();

    private async Task<DocumentStore> CreateStoreAsync()
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "daemon_change_listener";
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
            opts.Projections.AsyncListeners.Add(_listener);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        return theStore;
    }

    private async Task PublishStreamsAsync(DocumentStore store, int count)
    {
        await using var session = store.LightweightSession();
        for (var i = 0; i < count; i++)
        {
            session.Events.StartStream(Guid.NewGuid(),
                new QuestStarted($"Quest {i}"),
                new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas"]));
        }

        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task can_listen_for_commits_in_daemon()
    {
        var store = await CreateStoreAsync();
        await PublishStreamsAsync(store, 10);

        await store.WaitForProjectionAsync();

        // Both hooks fired, and the before hook always precedes the matching after hook.
        _listener.Befores.ShouldNotBeEmpty();
        _listener.Changes.ShouldNotBeEmpty();

        // The change set surfaces the projected aggregates that were written this batch.
        var changed = _listener.Changes
            .SelectMany(c => c.Inserted.Concat(c.Updated))
            .OfType<QuestParty>()
            .ToList();
        changed.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task listeners_are_not_active_in_rebuilds()
    {
        var store = await CreateStoreAsync();
        await PublishStreamsAsync(store, 10);

        // Normal continuous run fires the listeners.
        await store.WaitForProjectionAsync();
        _listener.Changes.ShouldNotBeEmpty();

        // Clear, then rebuild: a full replay must NOT re-fire post-commit side effects.
        _listener.Befores.Clear();
        _listener.Changes.Clear();

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        var projectionName = store.Options.Projections.All.Single().Name;
        await daemon.RebuildProjectionAsync(projectionName, CancellationToken.None);

        _listener.Befores.ShouldBeEmpty();
        _listener.Changes.ShouldBeEmpty();
    }
}

public class FakeChangeListener : IChangeListener
{
    public readonly List<IChangeSet> Befores = new();
    public readonly List<IChangeSet> Changes = new();

    public Task BeforeCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        session.ShouldNotBeNull();
        // BeforeCommit must run before its matching AfterCommit, so at capture time the after-list can
        // never be ahead of the before-list.
        Changes.Count.ShouldBeLessThanOrEqualTo(Befores.Count);
        Befores.Add(commit.Clone());
        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        session.ShouldNotBeNull();
        Changes.Add(commit.Clone());
        return Task.CompletedTask;
    }
}
