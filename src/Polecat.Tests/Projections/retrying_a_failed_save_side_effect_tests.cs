using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events.Aggregation;
using Polecat.Projections;
using Polecat.Services;
using Polecat.TestUtils;

namespace Polecat.Tests.Projections;

public record RetriedTallied(Guid Id);
public record RetriedNotice(Guid Id);

/// <summary>
///     Changes itself in place, so an event applied twice shows up in the count.
/// </summary>
public partial class RetriedTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }

    public void Apply(RetriedTallied e) => Count++;
}

public partial class PublishingTallyProjection : SingleStreamProjection<RetriedTally, Guid>
{
    public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<RetriedTally> slice)
    {
        if (slice.Snapshot is not null)
        {
            slice.PublishMessage(new RetriedNotice(slice.Snapshot.Id));
        }

        return ValueTask.CompletedTask;
    }
}

/// <summary>
///     Holds what it is given until the commit, which is what Wolverine's batch does. What it hands on
///     stays in <see cref="Published" />, so a batch used for two commits delivers the first one's
///     messages twice.
/// </summary>
public class BufferingMessageOutbox : IMessageOutbox
{
    public List<object> Delivered { get; } = [];
    public int Batches { get; private set; }

    public ValueTask<IMessageBatch> CreateBatch(IDocumentSession session)
    {
        Batches++;
        return new ValueTask<IMessageBatch>(new Batch(this));
    }

    private sealed class Batch(BufferingMessageOutbox outbox) : IMessageBatch
    {
        private readonly List<object> _published = [];

        public ValueTask PublishAsync<T>(T message, string tenantId)
        {
            _published.Add(message!);
            return ValueTask.CompletedTask;
        }

        public ValueTask PublishAsync<T>(T message, MessageMetadata metadata)
        {
            _published.Add(message!);
            return ValueTask.CompletedTask;
        }

        public Task BeforeCommitAsync(CancellationToken token) => Task.CompletedTask;

        public Task AfterCommitAsync(CancellationToken token)
        {
            outbox.Delivered.AddRange(_published);
            return Task.CompletedTask;
        }
    }
}

/// <summary>
///     Enlists a participant and queues an insert every time it is called, which is what a retry on the
///     same session does.
/// </summary>
public class EnlistsBeforeEverySave : IDocumentSessionListener
{
    public List<RetryCountingParticipant> Enlisted { get; } = [];
    public Guid? Insert { get; set; }

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
    {
        var participant = new RetryCountingParticipant();
        Enlisted.Add(participant);
        session.AddTransactionParticipant(participant);

        if (Insert is { } id)
        {
            session.Insert(new RetriedLog { Id = id });
        }

        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        => Task.CompletedTask;
}

public class FailsOnFirstBeforeSave : IDocumentSessionListener
{
    private int _calls;

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
    {
        if (Interlocked.Increment(ref _calls) == 1)
        {
            throw new InvalidOperationException("The first attempt at this save fails ahead of its transaction.");
        }

        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        => Task.CompletedTask;
}

/// <summary>
///     #916: what else a failed attempt at a save could leave behind for a retry on the same session,
///     after #870 dealt with the operations and participants its projections left.
/// </summary>
public class retrying_a_failed_save_side_effect_tests
{
    private const string Lightweight = nameof(Lightweight);
    private const string IdentityMap = nameof(IdentityMap);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<(DocumentStore Store, BufferingMessageOutbox Outbox)> CreateStoreAsync(string schema,
        Action<StoreOptions>? configure = null)
    {
        var outbox = new BufferingMessageOutbox();

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = schema;
            opts.Schema.For<RetriedLog>();
            opts.Events.MessageOutbox = outbox;
            opts.Events.EnableSideEffectsOnInlineProjections = true;
            opts.Projections.Add(new PublishingTallyProjection(), ProjectionLifecycle.Inline);
            configure?.Invoke(opts);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await store.Advanced.CleanAsync<RetriedTally>(Ct);
        await store.Advanced.CleanAsync<RetriedLog>(Ct);

        return (store, outbox);
    }

    private static IDocumentSession Open(DocumentStore store, string kind, SessionOptions? options = null)
        => kind == Lightweight
            ? store.LightweightSession(options ?? new SessionOptions())
            : store.IdentitySession(options ?? new SessionOptions());

    private static async Task<int?> StoredCountAsync(DocumentStore store, Guid id)
    {
        await using var query = store.QuerySession();
        return (await query.LoadAsync<RetriedTally>(id, Ct))?.Count;
    }

    [Theory]
    [InlineData(Lightweight)]
    [InlineData(IdentityMap)]
    public async Task a_projections_message_is_published_once_across_a_retry(string kind)
    {
        var (store, outbox) = await CreateStoreAsync("retry_message_" + kind.ToLowerInvariant());
        using var disposing = store;

        var id = Guid.NewGuid();

        await using var session = Open(store, kind);
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Events.StartStream(id, new RetriedTallied(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));
        outbox.Delivered.ShouldBeEmpty();

        await session.SaveChangesAsync(Ct);

        outbox.Delivered.ShouldHaveSingleItem().ShouldBe(new RetriedNotice(id));
    }

    /// <summary>
    ///     A batch belongs to one commit. Marten throws its own away for the same reason.
    /// </summary>
    [Fact]
    public async Task the_next_save_does_not_deliver_the_last_ones_messages_again()
    {
        var (store, outbox) = await CreateStoreAsync("retry_message_next_save");
        using var disposing = store;

        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        await using var session = store.LightweightSession();

        session.Events.StartStream(first, new RetriedTallied(first));
        await session.SaveChangesAsync(Ct);

        session.Events.StartStream(second, new RetriedTallied(second));
        await session.SaveChangesAsync(Ct);

        outbox.Batches.ShouldBe(2);
        outbox.Delivered.ShouldBe([new RetriedNotice(first), new RetriedNotice(second)]);
    }

    [Theory]
    [InlineData(Lightweight)]
    [InlineData(IdentityMap)]
    public async Task a_retry_folds_onto_what_is_stored_and_not_what_the_failed_attempt_built(string kind)
    {
        var (store, _) = await CreateStoreAsync("retry_fold_" + kind.ToLowerInvariant());
        using var disposing = store;

        var id = Guid.NewGuid();

        await using (var seed = store.LightweightSession())
        {
            seed.Events.StartStream(id, new RetriedTallied(id));
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = Open(store, kind);
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Events.Append(id, new RetriedTallied(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        (await StoredCountAsync(store, id)).ShouldBe(2);
    }

    [Theory]
    [InlineData(Lightweight)]
    [InlineData(IdentityMap)]
    public async Task a_retry_folds_correctly_with_the_document_already_loaded(string kind)
    {
        var (store, _) = await CreateStoreAsync("retry_fold_loaded_" + kind.ToLowerInvariant());
        using var disposing = store;

        var id = Guid.NewGuid();

        await using (var seed = store.LightweightSession())
        {
            seed.Events.StartStream(id, new RetriedTallied(id));
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = Open(store, kind);
        (await session.LoadAsync<RetriedTally>(id, Ct)).ShouldNotBeNull();

        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Events.Append(id, new RetriedTallied(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        (await StoredCountAsync(store, id)).ShouldBe(2);
    }

    private static async Task<Guid> SeedTallyAsync(DocumentStore store)
    {
        var id = Guid.NewGuid();

        await using var seed = store.LightweightSession();
        seed.Events.StartStream(id, new RetriedTallied(id));
        await seed.SaveChangesAsync(Ct);

        return id;
    }

    /// <summary>
    ///     The two caches a fold really can start from: the aggregate identity map, and for an Inline
    ///     aggregate the document identity map. With neither, <c>ProjectLatest</c> has a new instance
    ///     every time.
    /// </summary>
    [Theory]
    [InlineData(Lightweight, false)]
    [InlineData(Lightweight, true)]
    [InlineData(IdentityMap, false)]
    [InlineData(IdentityMap, true)]
    public async Task project_latest_after_a_failed_save_folds_the_pending_events_once(string kind, bool aggregateMap)
    {
        var (store, _) = await CreateStoreAsync($"retry_latest_{kind}_{aggregateMap}".ToLowerInvariant(),
            opts => opts.Projections.UseIdentityMapForAggregates = aggregateMap);
        using var disposing = store;

        var id = await SeedTallyAsync(store);

        await using var session = Open(store, kind);
        session.AddTransactionParticipant(new RetryFailsOnce());

        var stream = await session.Events.FetchForWriting<RetriedTally>(id, Ct);
        stream.AppendOne(new RetriedTallied(id));

        (await session.Events.ProjectLatest<RetriedTally>(id, Ct))!.Count.ShouldBe(2);

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        (await session.Events.ProjectLatest<RetriedTally>(id, Ct))!.Count.ShouldBe(2);

        await session.SaveChangesAsync(Ct);

        (await StoredCountAsync(store, id)).ShouldBe(2);
    }

    [Theory]
    [InlineData(Lightweight, false)]
    [InlineData(Lightweight, true)]
    [InlineData(IdentityMap, false)]
    [InlineData(IdentityMap, true)]
    public async Task project_latest_leaves_what_the_session_holds_as_it_found_it(string kind, bool aggregateMap)
    {
        var (store, _) = await CreateStoreAsync($"latest_holds_{kind}_{aggregateMap}".ToLowerInvariant(),
            opts => opts.Projections.UseIdentityMapForAggregates = aggregateMap);
        using var disposing = store;

        var id = await SeedTallyAsync(store);

        await using var session = Open(store, kind);

        var loaded = await session.LoadAsync<RetriedTally>(id, Ct);
        var stream = await session.Events.FetchForWriting<RetriedTally>(id, Ct);
        stream.AppendOne(new RetriedTallied(id));

        (await session.Events.ProjectLatest<RetriedTally>(id, Ct))!.Count.ShouldBe(2);
        (await session.Events.ProjectLatest<RetriedTally>(id, Ct))!.Count.ShouldBe(2);

        loaded!.Count.ShouldBe(1);
        stream.Aggregate!.Count.ShouldBe(1);
        (await session.LoadAsync<RetriedTally>(id, Ct))!.Count.ShouldBe(1);
    }

    /// <summary>
    ///     What is cached is still what comes back when there is nothing pending to fold.
    /// </summary>
    [Fact]
    public async Task project_latest_with_nothing_pending_still_answers_from_the_cache()
    {
        var (store, _) = await CreateStoreAsync("latest_cached",
            opts => opts.Projections.UseIdentityMapForAggregates = true);
        using var disposing = store;

        var id = await SeedTallyAsync(store);

        await using var session = store.LightweightSession();

        var stream = await session.Events.FetchForWriting<RetriedTally>(id, Ct);

        (await session.Events.ProjectLatest<RetriedTally>(id, Ct)).ShouldBeSameAs(stream.Aggregate);
    }

    [Fact]
    public async Task a_participant_enlisted_from_before_save_changes_commits_once_across_a_retry()
    {
        var (store, _) = await CreateStoreAsync("retry_listener_participant");
        using var disposing = store;

        var listener = new EnlistsBeforeEverySave();
        var options = new SessionOptions();
        options.Listeners.Add(listener);

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession(options);
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Events.StartStream(id, new RetriedTallied(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        listener.Enlisted.Sum(x => x.Commits).ShouldBe(1);
    }

    /// <summary>
    ///     This attempt never reached its transaction, which is a different way out of the save.
    /// </summary>
    [Fact]
    public async Task a_failure_ahead_of_the_transaction_abandons_what_an_earlier_listener_added()
    {
        var (store, _) = await CreateStoreAsync("retry_listener_before_tx");
        using var disposing = store;

        var logged = Guid.NewGuid();
        var listener = new EnlistsBeforeEverySave { Insert = logged };
        var options = new SessionOptions();
        options.Listeners.Add(listener);
        options.Listeners.Add(new FailsOnFirstBeforeSave());

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession(options);
        session.Events.StartStream(id, new RetriedTallied(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        listener.Enlisted.Sum(x => x.Commits).ShouldBe(1);

        await using var query = store.QuerySession();
        (await query.LoadAsync<RetriedLog>(logged, Ct)).ShouldNotBeNull();
    }

    /// <summary>
    ///     What the caller queued is not the attempt's, whichever way out it took.
    /// </summary>
    [Fact]
    public async Task what_the_caller_queued_survives_a_failure_ahead_of_the_transaction()
    {
        var (store, _) = await CreateStoreAsync("retry_keeps_before_tx");
        using var disposing = store;

        var options = new SessionOptions();
        options.Listeners.Add(new FailsOnFirstBeforeSave());

        var mine = new RetriedLog { Id = Guid.NewGuid() };
        var participant = new RetryCountingParticipant();

        await using var session = store.LightweightSession(options);
        session.AddTransactionParticipant(participant);
        session.Insert(mine);

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        participant.Commits.ShouldBe(1);

        await using var query = store.QuerySession();
        (await query.LoadAsync<RetriedLog>(mine.Id, Ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task an_operation_queued_from_before_save_changes_is_not_written_twice()
    {
        var (store, _) = await CreateStoreAsync("retry_listener_operation");
        using var disposing = store;

        var logged = Guid.NewGuid();
        var options = new SessionOptions();
        options.Listeners.Add(new EnlistsBeforeEverySave { Insert = logged });

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession(options);
        session.AddTransactionParticipant(new RetryFailsOnce());
        session.Events.StartStream(id, new RetriedTallied(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        await using var query = store.QuerySession();
        (await query.LoadAsync<RetriedLog>(logged, Ct)).ShouldNotBeNull();
    }
}
