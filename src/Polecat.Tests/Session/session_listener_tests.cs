using Polecat.Services;
using Polecat.Tests.Harness;
using Polecat.Tests.Linq;

namespace Polecat.Tests.Session;

[Collection("integration")]
public class session_listener_tests : IntegrationContext
{
    public session_listener_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task global_listener_before_save_is_called()
    {
        var listener = new TrackingListener();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "listener_test";
            opts.Listeners.Add(listener);
        });

        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "listener-test" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        listener.BeforeSaveCalled.ShouldBeTrue();
    }

    [Fact]
    public async Task global_listener_after_commit_is_called()
    {
        var listener = new TrackingListener();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "listener_test2";
            opts.Listeners.Add(listener);
        });

        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "listener-test" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        listener.AfterCommitCalled.ShouldBeTrue();
    }

    [Fact]
    public async Task session_listener_is_called()
    {
        var listener = new TrackingListener();

        await using var session = theStore.OpenSession(new SessionOptions
        {
            Listeners = { listener }
        });

        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "session-listener" };
        session.Store(doc);
        await session.SaveChangesAsync();

        listener.BeforeSaveCalled.ShouldBeTrue();
        listener.AfterCommitCalled.ShouldBeTrue();
    }

    [Fact]
    public async Task both_global_and_session_listeners_are_called()
    {
        var globalListener = new TrackingListener();
        var sessionListener = new TrackingListener();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "listener_test3";
            opts.Listeners.Add(globalListener);
        });

        await using var session = theStore.OpenSession(new SessionOptions
        {
            Listeners = { sessionListener }
        });

        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "both-listeners" };
        session.Store(doc);
        await session.SaveChangesAsync();

        globalListener.BeforeSaveCalled.ShouldBeTrue();
        globalListener.AfterCommitCalled.ShouldBeTrue();
        sessionListener.BeforeSaveCalled.ShouldBeTrue();
        sessionListener.AfterCommitCalled.ShouldBeTrue();
    }

    [Fact]
    public async Task listener_receives_session_with_pending_changes()
    {
        var listener = new InspectingListener();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "listener_test4";
            opts.Listeners.Add(listener);
        });

        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "inspect-pending" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        listener.PendingCountAtBeforeSave.ShouldBe(1);
        listener.PendingCountAtAfterCommit.ShouldBe(0); // cleared after commit
    }

    [Fact]
    public async Task listener_not_called_when_no_changes()
    {
        var listener = new TrackingListener();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "listener_test5";
            opts.Listeners.Add(listener);
        });

        // SaveChanges with no pending work
        await theSession.SaveChangesAsync();

        listener.BeforeSaveCalled.ShouldBeFalse();
        listener.AfterCommitCalled.ShouldBeFalse();
    }

    [Fact]
    public async Task after_commit_change_set_reports_inserts_updates_and_deletes()
    {
        var listener = new ChangeSetCapturingListener();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "listener_changeset";
            opts.Listeners.Add(listener);
        });

        // Seed a document we will update and one we will delete in the observed save.
        var toUpdate = new LinqTarget { Id = Guid.NewGuid(), Name = "before" };
        var toDelete = new LinqTarget { Id = Guid.NewGuid(), Name = "doomed" };
        await using (var seed = theStore.LightweightSession())
        {
            seed.Store(toUpdate);
            seed.Store(toDelete);
            await seed.SaveChangesAsync();
        }

        var inserted = new LinqTarget { Id = Guid.NewGuid(), Name = "fresh" };
        toUpdate.Name = "after";
        theSession.Insert(inserted);  // Insert role
        theSession.Update(toUpdate);  // Update role
        theSession.Delete(toDelete);  // Deletion role
        await theSession.SaveChangesAsync();

        listener.Commit.ShouldNotBeNull();
        var commit = listener.Commit!;

        commit.Inserted.OfType<LinqTarget>().Select(x => x.Id).ShouldContain(inserted.Id);
        commit.Updated.OfType<LinqTarget>().Select(x => x.Id).ShouldContain(toUpdate.Id);
        commit.Deleted.Select(x => x.Id).ShouldContain(toDelete.Id);
        commit.Deleted.ShouldAllBe(d => d.DocumentType == typeof(LinqTarget));
    }

    [Fact]
    public async Task listener_can_modify_pending_changes_in_before_save()
    {
        var extraDoc = new LinqTarget { Id = Guid.NewGuid(), Name = "added-by-listener" };
        var listener = new AddingListener(extraDoc);

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "listener_test6";
            opts.Listeners.Add(listener);
        });

        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "original" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // Both docs should be saved
        await using var query = theStore.QuerySession();
        var loaded1 = await query.LoadAsync<LinqTarget>(doc.Id);
        var loaded2 = await query.LoadAsync<LinqTarget>(extraDoc.Id);

        loaded1.ShouldNotBeNull();
        loaded2.ShouldNotBeNull();
        loaded2!.Name.ShouldBe("added-by-listener");
    }
}

public class TrackingListener : IDocumentSessionListener
{
    public bool BeforeSaveCalled { get; set; }
    public bool AfterCommitCalled { get; set; }

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
    {
        BeforeSaveCalled = true;
        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        AfterCommitCalled = true;
        return Task.CompletedTask;
    }
}

public class InspectingListener : IDocumentSessionListener
{
    public int PendingCountAtBeforeSave { get; set; }
    public int PendingCountAtAfterCommit { get; set; }

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
    {
        PendingCountAtBeforeSave = session.PendingChanges.Operations.Count;
        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        PendingCountAtAfterCommit = session.PendingChanges.Operations.Count;
        return Task.CompletedTask;
    }
}

public class ChangeSetCapturingListener : IDocumentSessionListener
{
    public IChangeSet? Commit { get; private set; }

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        => Task.CompletedTask;

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        // The live unit of work is reset after commit, so retain an immutable clone.
        Commit = commit.Clone();
        return Task.CompletedTask;
    }
}

public class AddingListener : IDocumentSessionListener
{
    private readonly LinqTarget _extraDoc;

    public AddingListener(LinqTarget extraDoc)
    {
        _extraDoc = extraDoc;
    }

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
    {
        session.Store(_extraDoc);
        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        return Task.CompletedTask;
    }
}
