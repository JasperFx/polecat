using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Services;
using Polecat.TestUtils;

namespace Polecat.Tests.Projections;

public record RetriedStarted(Guid Id);

public class RetriedLog
{
    public Guid Id { get; set; }
}

public class RetryFailsOnce : ITransactionParticipant
{
    private int _attempts;

    public int Attempts => _attempts;

    public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        if (Interlocked.Increment(ref _attempts) == 1)
        {
            throw new InvalidOperationException("The first attempt at this save fails.");
        }

        return Task.CompletedTask;
    }
}

public class RetryCountingParticipant : ITransactionParticipant, IAsyncDisposable
{
    public int Commits { get; private set; }
    public bool Disposed { get; private set; }

    public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        Commits++;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

public class FailsAfterCommit : IDocumentSessionListener
{
    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token) => Task.CompletedTask;

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        throw new InvalidOperationException("The commit went through, and this did not.");
    }
}

/// <summary>
///     Queues an <c>Insert</c>, and enlists a participant, every time it runs.
/// </summary>
public partial class EnlistingEventProjection : EventProjection
{
    public EnlistingEventProjection()
    {
        Name = "EnlistingEventProjection";
    }

    public List<RetryCountingParticipant> Enlisted { get; } = [];

    public void Project(RetriedStarted e, IDocumentSession ops)
    {
        ops.Insert(new RetriedLog { Id = e.Id });

        var participant = new RetryCountingParticipant();
        Enlisted.Add(participant);
        ops.AddTransactionParticipant(participant);
    }
}

/// <summary>
///     #870, for everything that is not EF Core: what a failed attempt at a save left behind for a retry
///     on the same session. The projections run again on that retry, so whatever they queued and enlisted
///     the first time has to go, and only that.
/// </summary>
public class retrying_a_failed_save_tests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<(DocumentStore Store, EnlistingEventProjection Projection)> CreateStoreAsync(string schema)
    {
        var projection = new EnlistingEventProjection();

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = schema;
            opts.Schema.For<RetriedLog>();
            opts.Projections.Add(projection, ProjectionLifecycle.Inline);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await store.Advanced.CleanAsync<RetriedLog>(Ct);

        return (store, projection);
    }

    [Fact]
    public async Task an_operation_a_projection_queued_is_not_written_twice()
    {
        var (store, _) = await CreateStoreAsync("retry_insert");
        using var disposing = store;

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Events.StartStream(id, new RetriedStarted(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        await using var query = store.QuerySession();
        (await query.LoadAsync<RetriedLog>(id, Ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task what_the_caller_queued_before_the_save_survives_the_failure()
    {
        var (store, _) = await CreateStoreAsync("retry_keeps_work");
        using var disposing = store;

        var mine = new RetriedLog { Id = Guid.NewGuid() };
        var id = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Insert(mine);
        session.Events.StartStream(id, new RetriedStarted(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        await using var query = store.QuerySession();
        (await query.LoadAsync<RetriedLog>(mine.Id, Ct)).ShouldNotBeNull();
        (await query.LoadAsync<RetriedLog>(id, Ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task a_participant_a_projection_enlisted_goes_with_the_attempt()
    {
        var (store, projection) = await CreateStoreAsync("retry_projection_participant");
        using var disposing = store;

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Events.StartStream(id, new RetriedStarted(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        var abandoned = projection.Enlisted.ShouldHaveSingleItem();
        abandoned.Disposed.ShouldBeTrue();

        await session.SaveChangesAsync(Ct);

        abandoned.Commits.ShouldBe(0);
        projection.Enlisted.Count.ShouldBe(2);
        projection.Enlisted[1].Commits.ShouldBe(1);
    }

    /// <summary>
    ///     Nothing runs again to enlist this one, so losing it would lose its work without a word. This
    ///     is Wolverine's outbox, which enlists itself before the save.
    /// </summary>
    [Fact]
    public async Task a_participant_enlisted_before_the_save_stays_for_the_retry()
    {
        var (store, _) = await CreateStoreAsync("retry_keeps_participant");
        using var disposing = store;

        var mine = new RetryCountingParticipant();
        var id = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.AddTransactionParticipant(mine);
        session.Events.StartStream(id, new RetriedStarted(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        mine.Disposed.ShouldBeFalse();

        await session.SaveChangesAsync(Ct);

        mine.Commits.ShouldBe(1);
    }

    /// <summary>
    ///     A failure after the commit is not a failed attempt. Nothing was rolled back, so nothing is
    ///     abandoned, and the work the session has already written stays forgotten.
    /// </summary>
    [Fact]
    public async Task a_failure_after_the_commit_abandons_nothing()
    {
        var (store, projection) = await CreateStoreAsync("retry_committed");
        using var disposing = store;

        var options = new SessionOptions();
        options.Listeners.Add(new FailsAfterCommit());

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession(options);
        session.Events.StartStream(id, new RetriedStarted(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        projection.Enlisted.ShouldHaveSingleItem().Disposed.ShouldBeFalse();

        await using var query = store.QuerySession();
        (await query.LoadAsync<RetriedLog>(id, Ct)).ShouldNotBeNull();
    }
}
