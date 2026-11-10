using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text;
using JasperFx;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Fetching;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Events.Aggregation;
using Polecat.Exceptions;
using Polecat.Internal.Operations;
using Polecat.Internal.Sessions;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Metadata;
using Polecat.Projections;
using Polecat.Schema.Identity.Sequences;
using Weasel.Core;
using Weasel.SqlServer;

// The `Events` property on this class shadows the Polecat.Events namespace, so the DCB types have to be
// reached by alias rather than by `Events.Dcb.X`.
using DcbBoundaryRead = Polecat.Events.Dcb.DcbBoundaryRead;
using DcbTagVersionCapture = Polecat.Events.Dcb.DcbTagVersionCapture;
using DcbTagVersionEntry = Polecat.Events.Dcb.DcbTagVersionEntry;
using DcbTagVersionPhase = Polecat.Events.Dcb.DcbTagVersionPhase;

namespace Polecat.Internal;

/// <summary>
///     Base class for document sessions. Handles operation queueing, event stream
///     processing, and SaveChangesAsync. Uses IAlwaysConnectedLifetime for
///     persistent connection + transaction management.
/// </summary>
internal abstract class DocumentSessionBase : QuerySession, IDocumentSession
{
    private readonly WorkTracker _workTracker = new();
    private readonly IInlineProjection<IDocumentSession>[] _inlineProjections;
    private readonly IReadOnlyList<IDocumentSessionListener> _sessionListeners;
    private readonly IReadOnlyList<JasperFx.Events.Documents.IDocumentCommitListener> _sessionCommitListeners;
    private readonly IAlwaysConnectedLifetime _transactional;
    private readonly List<ITransactionParticipant> _transactionParticipants = new();
    private ITransactionParticipantRegistrar? _participantOwner;
    private EventOperations? _eventOperations;

    protected DocumentSessionBase(
        StoreOptions options,
        IAlwaysConnectedLifetime lifetime,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        IInlineProjection<IDocumentSession>[] inlineProjections,
        string tenantId,
        IReadOnlyList<IDocumentSessionListener>? sessionListeners = null,
        IReadOnlyList<JasperFx.Events.Documents.IDocumentCommitListener>? sessionCommitListeners = null)
        : base(options, lifetime, providers, tableEnsurer, eventGraph, tenantId)
    {
        _transactional = lifetime;
        _inlineProjections = inlineProjections;
        _sessionListeners = sessionListeners ?? Array.Empty<IDocumentSessionListener>();
        _sessionCommitListeners = sessionCommitListeners
                                  ?? Array.Empty<JasperFx.Events.Documents.IDocumentCommitListener>();
    }

    public IWorkTracker PendingChanges => _workTracker;

    // #478 / jasperfx#674: aggregate snapshots this session's FetchForWriting calls owe back to the
    // second-level write cache, published once the commit succeeds. Null until an enrolled type is
    // actually fetched, which is the overwhelmingly common case.
    private List<PendingAggregateCacheWrite>? _aggregateCacheWrites;

    private readonly record struct PendingAggregateCacheWrite(
        IAggregateWriteCache Cache,
        AggregateCacheKey Key,
        object Aggregate,
        long Version);

    /// <summary>
    ///     Enlist an aggregate snapshot to be published to the write cache when this session commits.
    /// </summary>
    /// <remarks>
    ///     Deferred rather than written at fetch time on purpose; see the call site in
    ///     <see cref="Polecat.Events.EventOperations" /> for why take-on-read requires it.
    /// </remarks>
    internal void RegisterAggregateWriteCacheEntry(IAggregateWriteCache cache, AggregateCacheKey key,
        object aggregate, long version)
    {
        _aggregateCacheWrites ??= new List<PendingAggregateCacheWrite>();
        _aggregateCacheWrites.Add(new PendingAggregateCacheWrite(cache, key, aggregate, version));
    }

    /// <summary>
    ///     Publish the enlisted aggregate snapshots. Called only after a successful commit, so a
    ///     rolled-back unit of work leaves no entry rather than a poisoned one.
    /// </summary>
    private void StoreAggregateWriteCacheEntries()
    {
        if (_aggregateCacheWrites is not { Count: > 0 }) return;

        foreach (var pending in _aggregateCacheWrites)
        {
            pending.Cache.Store(pending.Key, pending.Aggregate, pending.Version);

            // Publishing is a hand-off, so this session gives up its own handle on the instance.
            // Without this, an aggregate could be reachable from BOTH the cache (where another
            // session may take it) and this session's aggregate identity map (where ProjectLatest
            // folds pending events onto whatever it finds) -- two live references to one mutable
            // object, which is precisely what take-on-read exists to prevent. Only reachable with
            // UseIdentityMapForAggregates also turned on, and a no-op otherwise.
            EvictAggregateFromIdentityMap(pending.Key.DocumentType, pending.Key.Id);
        }

        _aggregateCacheWrites.Clear();
    }

    Polecat.Events.IQueryEventStore IQuerySession.Events => EventOps;
    public new Polecat.Events.IEventOperations Events => EventOps;

    private EventOperations EventOps
    {
        get
        {
            // #462: every event operation -- Append, StartStream, AppendOptimistic -- reaches the
            // session through here, so one guard covers the whole event-write surface.
            assertNotDisposed();
            return _eventOperations ??= new EventOperations(this, _eventGraph, Options, _workTracker, TenantId);
        }
    }

    internal WorkTracker WorkTracker => _workTracker;

    internal EventGraph EventGraph => _eventGraph;

    /// <summary>
    ///     The <c>pc_dcb_tag_version</c> rows this session's boundaries read, held until save (gh-515).
    /// </summary>
    /// <remarks>
    ///     Held rather than queued as operations because a DCB boundary is a condition on an APPEND. A
    ///     session that reads a boundary, decides there is nothing to do, and saves has written nothing
    ///     for the condition to guard — and asserting it anyway would bump the row, invalidating every
    ///     concurrent session's boundary over a save that changed nothing.
    /// </remarks>
    private List<DcbTagVersionEntry>? _capturedDcbRows;

    /// <summary>
    ///     The boundaries whose events the save has to look at again, because a tag they read has no row
    ///     (#890). Usually empty.
    /// </summary>
    private List<DcbBoundaryRead>? _capturedDcbReads;

    internal void CaptureDcbBoundary(IReadOnlyList<DcbTagVersionEntry> rows, int eventsRead)
    {
        if (rows.Count == 0) return;

        _capturedDcbRows ??= new List<DcbTagVersionEntry>(rows.Count);
        _capturedDcbRows.AddRange(rows);

        if (DcbTagVersionCapture.ReadFor(rows, eventsRead) is { } read)
        {
            (_capturedDcbReads ??= []).Add(read);
        }
    }

    /// <summary>
    ///     Run the DCB tag-version phase for this save: assert the boundaries the session read, bump the
    ///     rows it appends under, and throw if any boundary lost its race (gh-515).
    /// </summary>
    private async Task AssertDcbBoundariesAsync(CancellationToken token)
    {
        // The overwhelming majority of saves never touch a tag at all, so this returns before walking
        // the streams.
        var captured = _capturedDcbRows;
        var anyTags = _eventGraph.TagTypes.Count > 0;
        if (!anyTags || (captured is not { Count: > 0 } && !_workTracker.Streams.Any(s => s.Events.Count > 0)))
        {
            _capturedDcbRows?.Clear();
            _capturedDcbReads?.Clear();
            return;
        }

        var appended = CollectAppendedTagRows();

        // A boundary guards an APPEND. A session that read one and then wrote nothing has nothing for it
        // to guard, and asserting anyway would bump the row and invalidate every concurrent session's
        // boundary over a save that changed nothing. The gate is "does this save append anything", not
        // "does it append under a tag" — matching Marten, so a boundary means the same thing in both
        // stores.
        var appendsEvents = _workTracker.Streams.Any(s => s.Events.Count > 0);

        // .ToArray() because `captured` IS _capturedDcbRows: the clear below would otherwise empty the
        // very list being handed to the phase, leaving every save with bumps and no assertions — which
        // is to say no boundary check at all.
        var capturedRows = appendsEvents && captured is { Count: > 0 }
            ? captured.ToArray()
            : Array.Empty<DcbTagVersionEntry>();
        var capturedReads = appendsEvents && _capturedDcbReads is { Count: > 0 }
            ? _capturedDcbReads.ToArray()
            : Array.Empty<DcbBoundaryRead>();

        // The captures belong to the unit of work that is ending here either way -- a retry after a
        // conflict re-fetches the boundary and captures it again.
        _capturedDcbRows?.Clear();
        _capturedDcbReads?.Clear();

        var phase = DcbTagVersionPhase.Build(_eventGraph, capturedRows, appended, capturedReads);
        if (phase == null) return;

        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        phase.WriteCommands(builder);
        builder.Compile();

        IReadOnlyList<DcbTagVersionEntry> losers;
        await using (var reader = (await ExecuteReaderAsync(batch, token).ConfigureAwait(false)))
        {
            losers = await phase.ReadLosersAsync(reader, token).ConfigureAwait(false);
        }

        if (losers.Count == 0) return;

        // #394: a session with a single violated boundary — overwhelmingly the common case — throws the
        // DcbConcurrencyException directly, matching Marten, so the documented `catch
        // (DcbConcurrencyException)` retry pattern ports between the two stores. Only a session that
        // violates several at once still wraps them, since there is more than one failure to carry.
        var exceptions = losers
            .Select(x => (Exception)new DcbConcurrencyException(x.Query!, x.LastSeenSequence))
            .ToList();

        if (exceptions.Count == 1) throw exceptions[0];

        throw new AggregateException(exceptions);
    }

    /// <summary>
    ///     The distinct tag rows this save appends under — the producer side of the boundary check, and
    ///     what makes a plain <c>Events.Append</c> of a tagged event invalidate someone else's in-flight
    ///     boundary rather than sliding past it.
    /// </summary>
    private List<DcbTagVersionEntry> CollectAppendedTagRows()
    {
        var rows = new List<DcbTagVersionEntry>();
        var seen = new HashSet<(string, string, string)>();

        foreach (var stream in _workTracker.Streams)
        {
            foreach (var @event in stream.Events)
            {
                if (@event.Tags is not { Count: > 0 }) continue;

                var tenantId = @event.TenantId ?? stream.TenantId ?? TenantId;

                foreach (var tag in @event.Tags)
                {
                    var registration = _eventGraph.FindTagType(tag.TagType);
                    if (registration == null) continue;

                    var value = JasperFx.Events.Tags.TagValueStringifier.Stringify(registration.ExtractValue(tag.Value));
                    if (seen.Add((registration.TableSuffix, value, tenantId)))
                    {
                        rows.Add(new DcbTagVersionEntry(
                            registration.TableSuffix, value, tenantId, null, null, 0));
                    }
                }
            }
        }

        return rows;
    }

    /// <summary>
    ///     Access the transactional connection's active transaction (if any).
    /// </summary>
    internal SqlTransaction? ActiveTransaction => _transactional.Transaction;

    /// <summary>
    ///     Refuse to go on if the transaction this session is inside has been ended by the server.
    /// </summary>
    internal void AssertTransactionIsUsable() => _transactional.AssertTransactionIsUsable();

    /// <summary>
    ///     Decides which SQL this session's unguarded document writes use (#854).
    /// </summary>
    internal bool RunsAtSnapshotIsolation => _transactional.IsolationLevel == System.Data.IsolationLevel.Snapshot;

    /// <summary>
    ///     Run a command that does not go through this session's lifetime but belongs to its unit of
    ///     work: today, what an EF Core <c>DbContext</c> does on the session's behalf (#846).
    /// </summary>
    /// <remarks>
    ///     Such a command runs on this session's connection and inside its transaction without ever
    ///     passing through <see cref="TransactionalConnection" />, so it would otherwise get neither of
    ///     the two things that type's callers rely on. One is the resilience pipeline. The other is
    ///     #832's check, and that one matters more here: <c>SqlCommand</c> quietly drops a completed
    ///     transaction, so a read or a flush arriving after the server ended the transaction would run
    ///     in autocommit. The check sits inside the pipeline for the reason given on
    ///     <see cref="TransactionalConnection" />: a retry strategy replays the lambda, so the replay
    ///     meets the check too.
    /// </remarks>
    internal ValueTask<T> ExecuteInUnitOfWorkAsync<T>(Func<CancellationToken, ValueTask<T>> command,
        CancellationToken token)
    {
        return Options.ResiliencePipeline.ExecuteAsync(static (state, t) =>
        {
            state.session.AssertTransactionIsUsable();
            return state.command(t);
        }, (session: this, command), token);
    }

    /// <inheritdoc cref="ExecuteInUnitOfWorkAsync{T}" />
    internal T ExecuteInUnitOfWork<T>(Func<T> command)
    {
        return Options.ResiliencePipeline.Execute(static state =>
        {
            state.session.AssertTransactionIsUsable();
            return state.command();
        }, (session: this, command));
    }

    public void AddTransactionParticipant(ITransactionParticipant participant)
    {
        if (_participantOwner != null)
        {
            _participantOwner.AddTransactionParticipant(participant);
            return;
        }

        _transactionParticipants.Add(participant);
    }

    /// <summary>
    ///     Send every participant registered on this session to <paramref name="owner" />, which will
    ///     commit this session's work in its place and is what has to run and dispose them.
    /// </summary>
    internal void HandParticipantsTo(ITransactionParticipantRegistrar owner)
    {
        _participantOwner = owner;
    }

    /// <summary>
    ///     Drain the transaction participants, then dispose the session itself.
    /// </summary>
    /// <remarks>
    ///     #650: a participant may own resources whose lifetime is its transaction's — Polecat.EntityFrameworkCore's
    ///     <c>DbContextTransactionParticipant</c> owns the <c>DbContext</c> an EF Core projection's storage was
    ///     built around, and nothing disposed it on any path. <c>ITransactionParticipant</c> declares no disposal
    ///     contract, so this only reaches the ones that opt into <see cref="IAsyncDisposable" />; the rest are
    ///     untouched.
    ///     <para>
    ///     A participant belongs to whatever commits its transaction. This session owns the ones it will commit
    ///     itself, which is the inline path, and whoever opened the session disposes it. A session working for
    ///     <c>PolecatProjectionBatch</c> holds none: the batch commits, so <see cref="HandParticipantsTo" /> sends
    ///     them there and <c>PolecatProjectionBatch.DisposeAsync</c> drains them (#867). That cannot be left to
    ///     this method, because disposing the session does not mean the commit has happened. JasperFx disposes
    ///     an event projection's session as soon as <c>ApplyAsync</c> returns, and draining there lost the
    ///     participant and every write it was carrying.
    ///     </para>
    ///     <para>
    ///     Draining is idempotent because the list is cleared, and a throw from one participant must not strand
    ///     the others, so each is disposed independently.
    ///     </para>
    /// </remarks>
    public override async ValueTask DisposeAsync()
    {
        List<Exception>? failures = null;

        foreach (var participant in _transactionParticipants)
        {
            if (participant is not IAsyncDisposable disposable) continue;

            try
            {
                await disposable.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                (failures ??= []).Add(e);
            }
        }

        _transactionParticipants.Clear();

        await base.DisposeAsync().ConfigureAwait(false);

        if (failures is not null)
        {
            throw failures.Count == 1 ? failures[0] : new AggregateException(failures);
        }
    }

    /// <remarks>
    ///     #832: this used to return early whenever <c>Transaction</c> was set, which is how a transaction
    ///     the server had rolled back passed for an open one. The lifetime tells them apart.
    /// </remarks>
    internal async Task BeginTransactionAsync(CancellationToken token)
    {
        await _transactional.BeginTransactionAsync(token).ConfigureAwait(false);
    }

    internal async Task EnsureTableForProviderAsync(DocumentProvider provider, CancellationToken token)
    {
        await _tableEnsurer.EnsureTablesAsync([provider], HasOpenTransaction, token).ConfigureAwait(false);
    }

    public void Store<T>(T document) where T : notnull
    {
        assertNotDisposed();
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        _workTracker.Add(BuildClosedShapeWrite(document, provider, WriteKind.Upsert));
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    // #273 E2b/E2c/E2e: every document write goes through the shared closed-shape operations
    // (wrapped for the bespoke unit-of-work currency until the op-currency flip); subclass
    // types resolve a SubClassPolecatStorage delegating to the hierarchy root's storage.
    // A tenant override (IDocumentSession.ForTenant) wraps the session in a
    // TenantScopedStorageSession so flush-time metadata binders write the override tenant.
    /// <param name="tenantScope">
    ///     polecat#548 — the ForTenant view's own tenant scope, when this write comes through one.
    ///     Supplied by the caller (rather than built here) so a nested tenant session's reads and
    ///     writes share one scope, and therefore one per-tenant identity map: Store() adds to the map
    ///     just as a load does, so building a fresh sharing wrapper per write would put another
    ///     tenant's document into the parent's map (marten#4801).
    /// </param>
    internal Operations.ClosedShapeOperationAdapter BuildClosedShapeWrite<T>(T document, DocumentProvider provider,
        WriteKind kind, Weasel.Storage.IStorageSession? tenantScope = null) where T : notnull
    {
        Weasel.Storage.IStorageSession session = tenantScope ?? (Weasel.Storage.IStorageSession)this;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();

        EnsureHiloStorage(provider);

        // id assignment + identity-map/version bookkeeping
        StoreForConcurrency(storage, session, document);

        var op = kind switch
        {
            WriteKind.Insert => storage.Insert(document, session, session.TenantId),
            WriteKind.Update => storage.Update(document, session, session.TenantId),
            _ => storage.Upsert(document, session, session.TenantId)
        };

        CaptureExpectedRevision(op, document, storage.MappedRevisionFor(document));

        return new Operations.ClosedShapeOperationAdapter(op, session, document, provider.Mapping.GetId(document));
    }

    /// <summary>
    ///     #883: the id assigned next may come from a Hi-Lo sequence, which cannot see this session's
    ///     transaction and so never waits for the migration lock on its own. This can.
    /// </summary>
    private void EnsureHiloStorage(DocumentProvider provider)
    {
        (provider.Sequence as HiloSequence)?.EnsureStorage(HasOpenTransaction);
    }

    /// <summary>
    ///     Runtime-type (object) variant of <see cref="BuildClosedShapeWrite{T}" /> for
    ///     StoreObjects (#273 E2e) — dispatches through the non-generic object-write bridge.
    /// </summary>
    internal Operations.ClosedShapeOperationAdapter BuildClosedShapeObjectWrite(object document,
        DocumentProvider provider, Weasel.Storage.IStorageSession? tenantScope = null)
    {
        Weasel.Storage.IStorageSession session = tenantScope ?? (Weasel.Storage.IStorageSession)this;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectWriteStorage)session.StorageFor(document.GetType());

        EnsureHiloStorage(provider);

        // #592: the same version seeding Store<T> does, through the object bridge.
        // #720 adds the mapped-member branch in the same order StoreForConcurrency uses it.
        if (document is IVersioned objectVersioned && objectVersioned.Version != Guid.Empty)
        {
            storage.StoreObject(session, document, objectVersioned.Version);
        }
        else if (storage.MappedVersionForObject(document) is { } mapped && mapped != Guid.Empty)
        {
            storage.StoreObject(session, document, mapped);
        }
        else
        {
            storage.StoreObject(session, document);
        }

        var op = storage.UpsertObject(document, session, session.TenantId);
        CaptureExpectedRevision(op, document, storage.MappedRevisionForObject(document));

        return new Operations.ClosedShapeOperationAdapter(op, session, document, provider.Mapping.GetId(document));
    }

    /// <summary>
    ///     #592 — seed the concurrency guard from the DOCUMENT's own version, not only from what this
    ///     session happens to have read.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Polecat's Guid optimistic concurrency used to work only inside one session. The guarded
    ///         MERGE binds its expected version out of <c>IStorageSession.Versions</c>, and that
    ///         tracker is written by the SELECT — so a document loaded in one session and stored
    ///         through another had no entry, the guard bound <c>DBNull</c>, the matched branch never
    ///         fired, and the write came back as a <c>ConcurrencyException</c>. Every time, on an
    ///         unmodified row. That refused stale writes correctly and refused legitimate ones too,
    ///         which is exactly the request-per-session workflow the feature exists for.
    ///     </para>
    ///     <para>
    ///         A non-empty <see cref="IVersioned.Version" /> on the instance is the caller's
    ///         expectation, so it seeds the tracker. <see cref="Guid.Empty" /> means "never stored",
    ///         which must stay unseeded: the INSERT branch has no version to guard on. Mirrors
    ///         Marten's <c>DocumentSessionBase.storeEntity</c>, and the same shape as fisher#245 and
    ///         marten#5372 — the same field, found three times independently, which is why
    ///         jasperfx#819 built a shared suite for it.
    ///     </para>
    ///     <para>
    ///         Numeric revisions are NOT handled here. They travel on the operation itself through
    ///         <see cref="CaptureExpectedRevision" />, which already reads the document rather than
    ///         the session.
    ///     </para>
    ///     <para>
    ///         #720 — a version member named through <c>Metadata(m =&gt; m.Version.MapTo(...))</c> is
    ///         consulted next, through the <c>MappedVersionFor</c> seam (weasel#590). It comes
    ///         <em>after</em> <see cref="IVersioned" /> because the marker is the more specific
    ///         declaration, and <em>before</em> the unseeded fallback because that fallback is exactly
    ///         the bug: a guard that binds nothing never matches, so the write is refused on an
    ///         unmodified row. The <see cref="Guid.Empty" /> rule is the same one, applied in the same
    ///         place, for the same reason — this is the fourth independent sighting of this field
    ///         (fisher#245, marten#5372, polecat#592, polecat#720), so it is deliberately one branch
    ///         rather than two.
    ///     </para>
    /// </remarks>
    private static void StoreForConcurrency<T>(Weasel.Storage.IDocumentStorage<T> storage,
        Weasel.Storage.IStorageSession session, T document) where T : notnull
    {
        if (document is IVersioned versioned && versioned.Version != Guid.Empty)
        {
            storage.Store(session, document, versioned.Version);
            return;
        }

        if (storage.MappedVersionFor(document) is { } mapped && mapped != Guid.Empty)
        {
            storage.Store(session, document, mapped);
            return;
        }

        storage.Store(session, document);
    }

    // Numeric revisions: the doc-carried version is the equality expectation (0 = new/auto),
    // matching the bespoke pipeline's expectedRevision capture. Shared with the projection
    // storage (#273 E2e).
    /// <param name="mappedRevision">
    ///     #720 — the document's <c>Metadata(m =&gt; m.Version.MapTo(...))</c> revision, from the
    ///     storage's <c>MappedRevisionFor</c> seam (weasel#590), or null when the caller has no
    ///     storage in hand or the mapping declares no such member. Consulted after the marker
    ///     interfaces and before the 0 default, mirroring <see cref="StoreForConcurrency{T}" />'s
    ///     ordering; 0 keeps meaning "new / auto-increment", so a mapped member that is still 0 falls
    ///     through to the same insert the marker-interface path takes.
    /// </param>
    internal static void CaptureExpectedRevision(Weasel.Storage.IStorageOperation op, object document,
        long? mappedRevision = null)
    {
        if (op is Weasel.Storage.IRevisionedOperation revisioned)
        {
            revisioned.Revision = ExpectedRevisionOf(document, mappedRevision);
        }
    }

    /// <summary>
    ///     The revision a document names, which is what an operation expects. 0 asks for the next one.
    /// </summary>
    internal static long ExpectedRevisionOf(object document, long? mappedRevision) => document switch
    {
        ILongVersioned longVersioned => longVersioned.Version,
        IRevisioned rev => rev.Version,
        _ => mappedRevision ?? 0
    };

    internal enum WriteKind
    {
        Upsert,
        Insert,
        Update
    }


    public void Store<T>(params T[] documents) where T : notnull
    {
        foreach (var doc in documents)
        {
            Store(doc);
        }
    }

    public void StoreObjects(IEnumerable<object> documents)
    {
        assertNotDisposed();
        // Per-document runtime-type dispatch through the closed-shape object-write bridge
        // (#273 E2e). Mirrors Store<T> otherwise.
        foreach (var document in documents)
        {
            if (document is null) continue;

            var documentType = document.GetType();
            SyncMetadata(document);
            var provider = _providers.GetProvider(documentType);
            _workTracker.Add(BuildClosedShapeObjectWrite(document, provider));
            OnDocumentStored(documentType, provider.Mapping.GetId(document), document);
        }
    }

    public void Insert<T>(T document) where T : notnull
    {
        assertNotDisposed();
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        _workTracker.Add(BuildClosedShapeWrite(document, provider, WriteKind.Insert));
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Update<T>(T document) where T : notnull
    {
        assertNotDisposed();
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        _workTracker.Add(BuildClosedShapeWrite(document, provider, WriteKind.Update));
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Delete<T>(T document) where T : notnull
    {
        assertNotDisposed();
        var provider = _providers.GetProvider<T>();
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        var deletion = storage.DeleteForDocument(document, TenantId);
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            deletion, session, document, provider.Mapping.GetId(document)));

        // Sync ISoftDeleted properties in memory
        if (document is ISoftDeleted softDeleted && provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            softDeleted.Deleted = true;
            softDeleted.DeletedAt = DateTimeOffset.UtcNow;
        }
    }

    public void Delete<T>(Guid id) where T : notnull
    {
        DeleteByObjectId<T>(id);
    }

    public void Delete<T>(string id) where T : notnull
    {
        DeleteByObjectId<T>(id);
    }

    public void Delete<T>(int id) where T : class
    {
        DeleteByObjectId<T>(id);
    }

    public void Delete<T>(long id) where T : class
    {
        DeleteByObjectId<T>(id);
    }

    // #273 E2c/E2e: all by-id deletions route through the closed-shape storage layer.
    private void DeleteByObjectId<T>(object id) where T : notnull
    {
        assertNotDisposed();
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.DeletionForObjectId(id, TenantId), session, id, id));
    }

    // #273 E2e: hard deletions route through the closed-shape storage layer.
    public void HardDelete<T>(T document) where T : notnull
    {
        assertNotDisposed();
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeleteForDocument(document, TenantId), session, document,
            _providers.GetProvider<T>().Mapping.GetId(document)));
    }

    public void HardDelete<T>(Guid id) where T : notnull => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(string id) where T : notnull => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(int id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(long id) where T : class => HardDeleteByObjectId<T>(id);

    private void HardDeleteByObjectId<T>(object id) where T : notnull
    {
        assertNotDisposed();
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeletionForObjectId(id, TenantId), session, id, id));
    }

    public void DeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : notnull
    {
        var fragment = ParseDeleteWhere(predicate);
        // #273 doc-side convergence: the DELETE / soft-delete-UPDATE prefix and tenancy come from
        // the shared closed-shape storage. DeleteFragment is already soft-or-hard per the type.
        var storage = ClosedShapeDeletionStorageFor<T>();
        // #419: when DeleteFragment is the soft-delete UPDATE, exclude rows that are already
        // deleted so the operation cannot re-stamp their deleted_at.
        _workTracker.Add(new DeleteWhereOperation(
            storage.DeleteFragment, storage.IsConjoined, TenantId, fragment, typeof(T),
            excludeAlreadyDeleted: storage.IsSoftDeleted));
    }

    public void HardDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : notnull
    {
        var fragment = ParseDeleteWhere(predicate);
        var storage = ClosedShapeDeletionStorageFor<T>();
        // #419: deliberately no is_deleted guard -- a hard delete of an already-soft-deleted row
        // is a real delete and must proceed.
        _workTracker.Add(new DeleteWhereOperation(
            storage.HardDeleteFragment, storage.IsConjoined, TenantId, fragment, typeof(T)));
    }

    public void UndoDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : notnull
    {
        var fragment = ParseDeleteWhere(predicate);
        var storage = ClosedShapeDeletionStorageFor<T>();
        // gh-558: UndeleteFragment is an UPDATE of is_deleted / deleted_at, columns DocumentTable
        // only adds for a soft-deleted type. Refused here, naming the type, rather than left to
        // fail at SaveChangesAsync as SQL Server's "Invalid column name 'is_deleted'". Marten and
        // Fisher both guard the same call.
        if (!storage.IsSoftDeleted)
        {
            throw new InvalidOperationException(
                $"'{typeof(T).FullNameInCode()}' is not configured for soft deletes, so a delete removed "
                + "the row outright and there is nothing to undo. Mark it with [SoftDeleted], implement "
                + $"ISoftDeleted, or call StoreOptions.Schema.For<{typeof(T).NameInCode()}>().SoftDeleted() "
                + "if it should be.");
        }

        _workTracker.Add(new UndoDeleteWhereOperation(
            storage.UndeleteFragment, storage.IsConjoined, TenantId, fragment, typeof(T)));
    }

    /// <summary>
    ///     #813: restore one soft-deleted row by its stored id, for
    ///     <see cref="PolecatDocumentDiagnosticsWriter.UndeleteDocumentAsync" />. The same
    ///     <see cref="Operations.UndoDeleteWhereOperation" /> <c>UndoDeleteWhere</c> queues, with an id
    ///     predicate in place of a parsed LINQ one (the writer has a type name and an id as text, not
    ///     an expression) and the version moved on. The caller has already refused a type that is not
    ///     soft-deleted.
    /// </summary>
    internal void UndeleteByIdForDiagnostics<T>(object id, System.Data.SqlDbType idType) where T : notnull
    {
        assertNotDisposed();
        var storage = ClosedShapeDeletionStorageFor<T>();
        _workTracker.Add(new Operations.UndoDeleteWhereOperation(
            storage.UndeleteFragment, storage.IsConjoined, TenantId, new IdEqualsFragment(id, idType), typeof(T),
            movesVersion: true));
    }

    private sealed class IdEqualsFragment : Linq.SqlGeneration.ISqlFragment
    {
        private readonly object _id;
        private readonly System.Data.SqlDbType _type;

        public IdEqualsFragment(object id, System.Data.SqlDbType type)
        {
            _id = id;
            _type = type;
        }

        public void Apply(Weasel.SqlServer.ICommandBuilder builder)
        {
            builder.Append("id = ");
            builder.AppendParameter(_id, _type);
        }
    }

    private Linq.SqlGeneration.ISqlFragment ParseDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : notnull
    {
        // #462: the single entry every *Where deletion passes through before queueing work.
        assertNotDisposed();
        var provider = _providers.GetProvider<T>();
        var memberFactory = new MemberFactory(Options, provider.Mapping);
        return new WhereClauseParser(memberFactory).Parse(predicate.Body);
    }

    private Storage.ClosedShape.IPolecatDeletionStorage ClosedShapeDeletionStorageFor<T>() where T : notnull
        => (Storage.ClosedShape.IPolecatDeletionStorage)_providers.ClosedShapeGraph.StorageFor<T>().QueryOnly;

    private Dictionary<string, NestedTenantSession>? _byTenant;

    public ITenantOperations ForTenant(string tenantId)
    {
        assertNotDisposed();
        _byTenant ??= new Dictionary<string, NestedTenantSession>();

        if (_byTenant.TryGetValue(tenantId, out var tenantSession))
        {
            return tenantSession;
        }

        assertTenantIsInThisDatabase(tenantId);
        AssertTenantFits(tenantId);

        tenantSession = new NestedTenantSession(this, tenantId);
        _byTenant[tenantId] = tenantSession;
        return tenantSession;
    }

    /// <summary>
    ///     A view of a tenant shares this session's connection and transaction, so it can only reach this
    ///     session's database. For a tenant anywhere else it would read and write the wrong database, with
    ///     that tenant's id on the rows. Marten's <c>ForTenant</c> refuses at the same point. Writing to the
    ///     right database instead would need a second transaction, and a save that looks atomic but isn't
    ///     is no better.
    /// </summary>
    /// <remarks>
    ///     By the connection and not by <see cref="Storage.ITenancy.GetDatabase" />, for two reasons. A
    ///     session bound with <see cref="SessionOptions.ForDatabase(Storage.PolecatDatabase)" /> isn't on
    ///     its tenant's database, or has no tenant to speak of. And each tenant has a
    ///     <see cref="Storage.PolecatDatabase" /> of its own even when two of them share a physical
    ///     database, where Marten's <c>StaticMultiTenancy</c> would give them one. Those two are allowed.
    ///     An unknown or disabled tenant gets the same refusal as opening a session for it.
    /// </remarks>
    private void assertTenantIsInThisDatabase(string tenantId)
    {
        var tenancy = Options.Tenancy;
        if (tenantId == TenantId || tenancy is null
            || tenancy.Cardinality == JasperFx.Descriptors.DatabaseCardinality.Single) return;

        var connection = _lifetime.ConnectionFactory;
        if (!connection.ConnectsToSameDatabaseAs(tenancy.GetConnectionFactory(tenantId)))
        {
            throw new InvalidTenantForDatabaseException(tenantId, connection.DatabaseName);
        }
    }

    public void UpdateExpectedVersion<T>(T document, Guid version) where T : notnull
    {
        if (document is IVersioned versioned)
        {
            versioned.Version = version;
        }
        else
        {
            ApplyMappedVersion(document, version);
        }

        Store(document);
    }

    public void UpdateRevision<T>(T document, int revision) where T : notnull
    {
        if (document is IRevisioned revisioned)
        {
            revisioned.Version = revision;
        }
        else if (document is ILongVersioned longVersioned)
        {
            longVersioned.Version = revision;
        }
        else
        {
            ApplyMappedRevision(document, revision);
        }

        Store(document);
    }

    public void UpdateRevision<T>(T document, long revision) where T : notnull
    {
        if (document is ILongVersioned longVersioned)
        {
            longVersioned.Version = revision;
        }
        else
        {
            ApplyMappedRevision(document, revision);
        }

        Store(document);
    }

    /// <summary>
    ///     #720 — these two methods name the expected version by assigning it to the document and then
    ///     calling <see cref="Store{T}(T)" />, which is why a mapped member needs the write-back half as
    ///     well as the read: with nowhere for the assignment to land, the call silently degrades into a
    ///     plain <c>Store</c> at the unchanged value, and #559's strictly-greater rule then refuses it.
    /// </summary>
    private void ApplyMappedVersion<T>(T document, Guid version) where T : notnull
    {
        if (((Weasel.Storage.IStorageSession)this).StorageFor<T>() is
            Storage.ClosedShape.IPolecatMappedConcurrencyStorage<T> mapped)
        {
            mapped.TryApplyMappedVersion(document, version);
        }
    }

    private void ApplyMappedRevision<T>(T document, long revision) where T : notnull
    {
        if (((Weasel.Storage.IStorageSession)this).StorageFor<T>() is
            Storage.ClosedShape.IPolecatMappedConcurrencyStorage<T> mapped)
        {
            mapped.TryApplyMappedRevision(document, revision);
        }
    }

    public void QueueSqlCommand(string sql, params object[] parameterValues)
    {
        assertNotDisposed();
        var operation = new Operations.ExecuteSqlStorageOperation(sql, parameterValues);
        _workTracker.Add(operation);
    }

    public async Task SaveChangesAsync(CancellationToken token = default)
    {
        assertNotDisposed();

        // _workTracker tracks document operations and event streams - it does NOT include
        // ITransactionParticipants registered via AddTransactionParticipant. Skipping the entire
        // SaveChangesAsync pipeline when _workTracker is empty therefore silently drops queued
        // participants whose BeforeCommitAsync would otherwise run inside the same SQL transaction.
        //
        // This bit Wolverine.Polecat's scheduled-cascade outbox path (JasperFx/wolverine GH-2941):
        // a handler that emits only a DeliveryMessage<T>.DelayedFor(...) cascade has Wolverine
        // call Session.StoreIncoming(...), which registers a StoreIncomingEnvelopeParticipant
        // whose BeforeCommitAsync inserts the scheduled envelope row. With no doc ops on the
        // session, SaveChangesAsync early-returned and the row never got written.
        if (!_workTracker.HasOutstandingWork() && _transactionParticipants.Count == 0) return;

        using var activity = OpenTelemetry.TracingSessionDecorator.StartSessionActivity(
            "polecat.save_changes", TenantId, Options.OpenTelemetry);
        OpenTelemetry.TracingSessionDecorator.AddOperationEvents(
            activity, _workTracker.Operations, Options.OpenTelemetry);

        var started = Stopwatch.GetTimestamp();
        try
        {
            await SaveChangesInternalAsync(token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            OpenTelemetry.TracingSessionDecorator.RecordException(activity, ex);
            throw;
        }
        finally
        {
            var duration = OpenTelemetry.PolecatTracing.SaveChangesDuration;
            if (duration.Enabled) duration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }
    }

    private async Task SaveChangesInternalAsync(CancellationToken token)
    {
        // #870: anything queued or enlisted past this point belongs to this attempt at the save. #916: and
        // that starts ahead of the listeners, because a retry calls BeforeSaveChangesAsync again.
        var operationsBeforeThisAttempt = _workTracker.OperationCount;
        var participantsBeforeThisAttempt = _transactionParticipants.Count;
        var messageBatchBeforeThisAttempt = _inlineMessageBatch;
        var eventsBeforeThisAttempt = _workTracker.EventCountsByStream();

        bool reliesOnTransaction;
        try
        {
            // #832: before any listener or DDL runs on behalf of a save that cannot happen.
            _transactional.AssertWritesAreAllowed();
            await prepareToSaveChangesAsync(token).ConfigureAwait(false);

            // Was the caller already relying on this transaction before the save? An exclusive fetch's
            // lock or a Snapshot session's view of the data is that, and neither comes back if the server
            // ends the transaction. A transaction the save itself is about to begin holds nothing the
            // caller has seen, so losing it fails this save and nothing else.
            reliesOnTransaction = _transactional.Transaction != null || _transactional.HoldsTransactionForSession;

            await _transactional.BeginTransactionAsync(token).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // #783 (marten#5688): FetchForExclusiveWriting / AppendExclusive begin the session's
            // transaction, and take UPDLOCK, HOLDLOCK on the stream row, before SaveChangesAsync is called.
            // A save that fails or is cancelled before the `using var tx` below has not taken ownership of
            // that transaction, so it used to stay open, holding the stream lock, until the session was
            // disposed. End it here, as a failure after that point does.
            await abandonOpenTransactionAsync(e).ConfigureAwait(false);
            await abandonAttemptAsync(operationsBeforeThisAttempt, participantsBeforeThisAttempt,
                messageBatchBeforeThisAttempt, eventsBeforeThisAttempt).ConfigureAwait(false);
            throw;
        }

        using var tx = _transactional.Transaction!;
        var committed = false;

        try
        {
            // gh-515: the DCB boundary check, run BEFORE the events are inserted so this session's own
            // appends can't trip it. It asserts-and-bumps the pc_dcb_tag_version rows the session's
            // boundaries captured, and bumps the rows it merely appends under, in one sorted phase.
            await AssertDcbBoundariesAsync(token).ConfigureAwait(false);

            // Process event streams through the shared Weasel.Storage.EventStorage<TId> hierarchy
            // (SqlServerEventStoreDialect + PolecatQuickAppendEventsOperation) — #273.
            await ProcessStreamsClosedShapeAsync(token).ConfigureAwait(false);

            // Apply inline projections — projected document ops are queued into _workTracker
            if (_inlineProjections.Length > 0 && _workTracker.Streams.Count > 0)
            {
                var streams = _workTracker.Streams.ToList();
                foreach (var projection in _inlineProjections)
                {
                    await projection.ApplyAsync(this, streams, token).ConfigureAwait(false);
                }

                // Ensure document tables exist for any additional projected types. #868: this is the one
                // ensure a save cannot move ahead of its transaction, because the types are only known
                // once the projections have run. #870: so it asks this transaction's own connection first,
                // and only a table that really is missing costs a second one.
                if (_workTracker.Operations.Count > 0)
                {
                    var newTypes = _workTracker.Operations
                        .Select(op => op.DocumentType)
                        .Where(t => t != typeof(object))
                        .Distinct()
                        .Select(t => _providers.GetProvider(t));
                    await _tableEnsurer.EnsureProjectedTablesAsync(newTypes, tableExistsAsync, token).ConfigureAwait(false);
                }

                // #919: what the projections just queued. Nothing has been sent for it yet, and the
                // streams the projections came from roll back with the refusal.
                await AssertStringKeysFitAsync(token).ConfigureAwait(false);
            }

            // Process document operations using BatchBuilder/SqlBatch. gh-515: the DCB boundary check is
            // no longer an operation in this list — it is held on the session and run as its own phase
            // above — so there is nothing to filter out here any more.
            var remainingOps = _workTracker.Operations.ToList();
            if (remainingOps.Count > 0)
            {
                await using var batch = new SqlBatch();
                var builder = new BatchBuilder(batch);

                var operations = remainingOps;
                for (var i = 0; i < operations.Count; i++)
                {
                    if (i > 0) builder.StartNewCommand();
                    Operations.StorageOperationExecution.Configure(
                        operations[i], builder, (Weasel.Storage.IStorageSession)this);
                }

                builder.Compile();

                try
                {
                    var exceptions = new List<Exception>();
                    await using var reader = (await ExecuteReaderAsync(batch, token).ConfigureAwait(false));
                    await OperationResultSets.ApplyAsync(reader, operations, exceptions, token).ConfigureAwait(false);

                    if (exceptions.Count == 1)
                    {
                        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exceptions[0]).Throw();
                    }

                    if (exceptions.Count > 0)
                    {
                        throw new AggregateException(exceptions);
                    }
                }
                catch (SqlException ex)
                {
                    // #549: give a queued operation the chance to name its own SQL error, exactly as
                    // the daemon's batch flush does (PolecatProjectionBatch.ExecuteAsync, since #916).
                    //
                    // #849: this is also where the application's transforms are asked, so it comes
                    // before the 2627 below for the same reason it does in Marten: one that claims an
                    // error Polecat translates replaces that translation.
                    if (BatchExceptionTransforms.TryTransform(ex, batch, operations, Options.ExceptionTransforms,
                            out var transformed))
                    {
                        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(transformed!).Throw();
                    }

                    // Map duplicate key violation to DocumentAlreadyExistsException
                    if (ex.Number == 2627
                        && operations.FirstOrDefault(op => op.Role() == OperationRole.Insert) is { } insertOp)
                    {
                        var documentId = insertOp is IStorageOperation bespoke ? bespoke.DocumentId : null;
                        throw new DocumentAlreadyExistsException(insertOp.DocumentType, documentId!);
                    }

                    throw;
                }
            }

            // Call transaction participants (e.g., EF Core DbContext) before commit
            foreach (var participant in _transactionParticipants)
            {
                await participant.BeforeCommitAsync(_transactional.Connection!, tx, token).ConfigureAwait(false);
            }

            if (_inlineMessageBatch is not null)
            {
                await _inlineMessageBatch.BeforeCommitAsync(token).ConfigureAwait(false);
            }

            await SqlTransactionRequests.CommitAsync(tx, token).ConfigureAwait(false);

            // #832: cleared here rather than in the finally. A committed SqlTransaction reports a null
            // Connection exactly as a dead one does, so a post-commit hook that reads through this session
            // must not find it still in place. #844: and the read it makes on a Serializable or Snapshot
            // session begins the next transaction, which the finally must leave alone.
            committed = true;
            _transactional.Transaction = null;

            // #916: a batch belongs to one commit. Kept, the next save on this session would run its
            // hooks again and deliver what it holds a second time. Marten throws its own away here too.
            var messageBatch = _inlineMessageBatch;
            _inlineMessageBatch = null;

            // #478: the aggregate write cache is published here and nowhere else. After the commit,
            // so a failed one leaves nothing behind; before the tracker is reset, purely to keep it
            // beside the other post-commit work.
            StoreAggregateWriteCacheEntries();

            // Runtime append observation (polecat#213 / CritterWatch#500) — capture the committed
            // events before the work tracker is reset, then notify best-effort.
            NotifyAppendObserver();

            // #238: emit the opt-in polecat.event.append OpenTelemetry counter, also before reset.
            RecordEventAppendMetrics();

            // Snapshot the unit of work as an IChangeSet before the tracker is reset, so AfterCommit
            // listeners can see what was inserted/updated/deleted in this save.
            var commit = _workTracker.Clone();

            _workTracker.Reset();

            Logger.RecordSavedChanges(this);

            // #783: everything from here on runs after a COMMIT that has already happened, so none of
            // it gets the caller's token. A token cancelled now — a client that disconnects as the
            // response is written, which is what WolverineFx.Http passes in as RequestAborted — would
            // make any token-honouring listener or the outbox throw, and SaveChangesAsync would report a
            // committed unit of work as failed while skipping every hook after that one. The daemon's
            // projection batch made the same call for its own post-commit hooks in #744. A hook that
            // genuinely fails still fails the call, exactly as before; only cancellation stops reaching
            // past the commit.
            var afterCommit = CancellationToken.None;

            // #807: the participants' half of the commit, before any listener. It reconciles what
            // BeforeCommitAsync left pending rather than announcing anything, so a listener that throws
            // must not leave a participant believing its rows are still to be written. Every participant
            // ran BeforeCommitAsync inside the transaction that just committed: they stay enlisted until
            // the session ends, and run again on its next save.
            foreach (var participant in _transactionParticipants)
            {
                await participant.AfterCommitAsync(afterCommit).ConfigureAwait(false);
            }

            // Call AfterCommitAsync on all listeners (global then session)
            foreach (var listener in Options.Listeners)
            {
                await listener.AfterCommitAsync(this, commit, afterCommit).ConfigureAwait(false);
            }

            foreach (var listener in _sessionListeners)
            {
                await listener.AfterCommitAsync(this, commit, afterCommit).ConfigureAwait(false);
            }

            // #485 / jasperfx#679: the store-agnostic post-commit listeners, after the store-native
            // ones and on the SAME `commit` snapshot taken above -- so a consumer holding both kinds
            // sees one consistent view of the unit of work, and the shared contract cannot observe a
            // commit the native listeners did not.
            //
            // ⚠️ THIS LOOP is where the contract silently no-ops. Nothing about declaring
            // IDocumentChangeSet on ChangeSet is visible to a consumer until a listener is actually
            // invoked, and a store that declares every interface perfectly and never reaches here
            // compiles clean and passes every other compliance suite. That is what
            // DocumentCommitListenerCompliance exists to catch.
            //
            // Inside the `try`, after `await tx.CommitAsync`, and deliberately NOT in the `finally`
            // below: the contract requires the callback if and only if the commit succeeded, and a
            // rolled-back transaction that announced its writes is the one failure the suite pins
            // explicitly.
            foreach (var listener in Options.CommitListeners)
            {
                await listener.AfterCommitAsync(this, commit, afterCommit).ConfigureAwait(false);
            }

            foreach (var listener in _sessionCommitListeners)
            {
                await listener.AfterCommitAsync(this, commit, afterCommit).ConfigureAwait(false);
            }

            if (messageBatch is not null)
            {
                await messageBatch.AfterCommitAsync(afterCommit).ConfigureAwait(false);
            }
        }
        catch (Exception failure)
        {
            // Roll back here rather than leaving it to `using var tx`, whose Dispose would issue the
            // same request synchronously -- on a MARS connection, the thread-pool wait that
            // SqlTransactionRequests exists to avoid, paid by every session that lost a race. A
            // transaction that already committed has no connection left and needs nothing.
            if (tx.Connection is not null)
            {
                // #854: the same loss as the branch below, only requested from this end. Clearing
                // Transaction would let the next save carry on without the lock or the snapshot, and
                // the version check that still guards an append guards no document.
                if (reliesOnTransaction)
                {
                    _transactional.RefuseWrites(failure);
                }

                try
                {
                    await SqlTransactionRequests.RollbackAsync(tx).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.LogFailure("Failure trying to roll back after a failed SaveChangesAsync", e);
                }
            }
            else if (!committed && reliesOnTransaction)
            {
                // #832: the server ended it, and with it whatever the caller was relying on. Clearing
                // Transaction below would otherwise let the next save begin a clean transaction and
                // quietly carry on without the lock or the snapshot.
                _transactional.MarkSessionUnusable(failure);
            }

            if (!committed)
            {
                await abandonAttemptAsync(operationsBeforeThisAttempt, participantsBeforeThisAttempt,
                    messageBatchBeforeThisAttempt, eventsBeforeThisAttempt).ConfigureAwait(false);
            }

            throw;
        }
        finally
        {
            if (ReferenceEquals(_transactional.Transaction, tx))
            {
                _transactional.Transaction = null;
            }

            // Drop anything a failed commit left enlisted. Harmless either way -- an entry is stored
            // at the version the instance reflects, so it would still be correct on a later commit --
            // but a session should not carry a fetch's leftovers past the save that failed.
            _aggregateCacheWrites?.Clear();
        }
    }

    /// <summary>
    ///     Is this table there, as far as the transaction this save holds can tell? On the session's own
    ///     connection, so it needs nothing from the pool, and any wait on it is one the server can see.
    /// </summary>
    private async Task<bool> tableExistsAsync(string qualifiedTableName, CancellationToken token)
    {
        await using var cmd = new SqlCommand("SELECT OBJECT_ID(@table);");
        cmd.Parameters.AddVarChar("@table", qualifiedTableName);

        return await ExecuteScalarAsync(cmd, token).ConfigureAwait(false) is not (null or DBNull);
    }

    /// <summary>
    ///     #870: forget what a failed attempt at a save added, because a retry on this session adds it
    ///     again. #916: that is the listeners' <c>BeforeSaveChangesAsync</c> as well as the projections,
    ///     and it includes the message batch, whose messages the projections publish again on the retry.
    ///     It needs no telling: <c>BeforeCommitAsync</c> wrote inside the transaction that just rolled back,
    ///     and <c>AfterCommitAsync</c> never ran, so nothing it holds was sent. And it includes the events a
    ///     listener appended, while what the application put on those same streams before the save stays,
    ///     because nothing on a retry would add that again.
    /// </summary>
    private async Task abandonAttemptAsync(int operationsBefore, int participantsBefore,
        IMessageBatch? messageBatchBefore, IReadOnlyDictionary<StreamAction, int> eventsBefore)
    {
        _workTracker.RemoveOperationsFrom(operationsBefore);
        _workTracker.RemoveEventsSince(eventsBefore);
        _inlineMessageBatch = messageBatchBefore;
        await abandonParticipantsFromAsync(participantsBefore).ConfigureAwait(false);
    }

    /// <summary>
    ///     #870: drop the participants a failed attempt at a save enlisted, from
    ///     <paramref name="firstOfTheAttempt" /> on.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         An inline projection enlists a participant each time it runs, and a retry on this session
    ///         runs it again. The attempt that failed is rolled back but nothing took its participant off
    ///         the list, so the retry flushed both: for Polecat.EntityFrameworkCore, two contexts holding
    ///         the same pending rows, which is a primary key violation at best and a row written twice
    ///         where the key is generated. The operations a projection queues have the same shape, which
    ///         is why the caller forgets those too: two <c>Insert</c>s of one document fail the retry.
    ///     </para>
    ///     <para>
    ///         Only the attempt's own. A participant enlisted before the save, as Wolverine's outbox does,
    ///         has nothing to enlist it again, and its work was rolled back with everything else, so it
    ///         stays for the retry. After a commit that went through, nothing here runs: those participants
    ///         stay until the session ends, for the reason given on <c>DbContextTransactionParticipant</c>.
    ///     </para>
    ///     <para>
    ///         Disposal has no token, and a failure here must not replace the save's own exception.
    ///     </para>
    /// </remarks>
    private async Task abandonParticipantsFromAsync(int firstOfTheAttempt)
    {
        var abandoned = _transactionParticipants.Count - firstOfTheAttempt;
        if (abandoned <= 0) return;

        var participants = _transactionParticipants.GetRange(firstOfTheAttempt, abandoned);
        _transactionParticipants.RemoveRange(firstOfTheAttempt, abandoned);

        foreach (var disposable in participants.OfType<IAsyncDisposable>())
        {
            try
            {
                await disposable.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.LogFailure("Failure trying to dispose a transaction participant after a failed SaveChangesAsync", e);
            }
        }
    }

    /// <summary>
    ///     The part of a save that runs before its transaction: the listeners' BeforeSaveChangesAsync, and
    ///     DDL and tenant-ordinal provisioning, which open connections of their own.
    /// </summary>
    private async Task prepareToSaveChangesAsync(CancellationToken token)
    {
        // Call BeforeSaveChangesAsync on all listeners (global then session)
        foreach (var listener in Options.Listeners)
        {
            await listener.BeforeSaveChangesAsync(this, token).ConfigureAwait(false);
        }

        foreach (var listener in _sessionListeners)
        {
            await listener.BeforeSaveChangesAsync(this, token).ConfigureAwait(false);
        }

        // Ensure document tables exist for pending operations (skip non-document ops like FlatTable)
        if (_workTracker.Operations.Count > 0)
        {
            var typesNeeded = _workTracker.Operations
                .Select(op => op.DocumentType)
                .Where(t => t != typeof(object))
                .Distinct()
                .Select(t => _providers.GetProvider(t));

            await _tableEnsurer.EnsureTablesAsync(typesNeeded, HasOpenTransaction, token).ConfigureAwait(false);
        }

        // #870: tables an earlier save's projections wrote to and only looked up, because it was already
        // inside its transaction by then. This is the first chance to do the rest of an ensure for them.
        if (!HasOpenTransaction)
        {
            await _tableEnsurer.EnsureTablesFoundInsideTransactionAsync(token).ConfigureAwait(false);
        }

        // Pre-create document tables for projected types that projections may query. #548: this set comes
        // entirely from configuration, so it is computed once per store and cached rather than re-derived
        // from Projections.All on every save. #868: it used to run inside the save's transaction, after
        // the appends, where it could not wait for the migration lock. Nothing about it needs to be there.
        if (_inlineProjections.Length > 0 && _workTracker.Streams.Count > 0)
        {
            await _tableEnsurer.EnsureTablesAsync(Options.Projections.InlineProjectedProviders, HasOpenTransaction,
                token).ConfigureAwait(false);
        }

        // #219: ensure the event store schema exists before appending — the event-sourcing analogue
        // of ensuring document tables above. Runs outside the data transaction (it opens its own
        // connection) and only when there are events to write. "Outside" means not in the transaction this
        // save begins. One begun earlier, by AppendExclusive or FetchForExclusiveWriting, is still open
        // here, which is why this says so (#868).
        if (_workTracker.Streams.Any(s => s.Events.Any()))
        {
            await _tableEnsurer.EnsureEventStoreSchemaAsync(HasOpenTransaction, token).ConfigureAwait(false);
        }

        // #919: past the ensures so the columns are there to ask about, and ahead of the provisioning
        // below so a refused save leaves nothing behind.
        await AssertStringKeysFitAsync(token).ConfigureAwait(false);

        // #335: tenant-partitioned documents — resolve (and lazily provision, mirroring the event
        // append path) the partition ordinal for every tenant this flush writes under, BEFORE the
        // data transaction opens (provisioning runs DDL on its own connection). The document write
        // SQL then resolves tenant_ordinal server-side from the pc_tenant_partitions registry these
        // provisions populate; an already-known tenant is a cached no-op. #864: an inline projection's
        // operations do not exist yet, they are queued inside the transaction, so its streams' tenants
        // have to be taken from the streams or the write finds no registry row.
        var projectsInline = _inlineProjections.Length > 0 && _workTracker.Streams.Any(s => s.Events.Any());
        if (Options.Policies.DocumentTenantPartitioningEnabled && (_workTracker.Operations.Count > 0 || projectsInline))
        {
            await _eventGraph.TenantOrdinals.ResolveAsync(TenantId, HasOpenTransaction, token).ConfigureAwait(false);
            foreach (var adapter in _workTracker.Operations.OfType<Operations.ClosedShapeOperationAdapter>())
            {
                await _eventGraph.TenantOrdinals.ResolveAsync(adapter.SessionTenantId, HasOpenTransaction, token).ConfigureAwait(false);
            }

            if (projectsInline)
            {
                foreach (var stream in _workTracker.Streams.Where(s => s.Events.Any()))
                {
                    await _eventGraph.TenantOrdinals.ResolveAsync(stream.TenantId, HasOpenTransaction, token).ConfigureAwait(false);
                }
            }
        }

        // #864: the event half of the same rule. Provisioning a new tenant SPLITs pc_streams on a
        // connection of its own, which needs a schema-modification lock, so it cannot wait until the
        // append planner's locking read has taken that table: the SPLIT would wait on this save's own
        // transaction until it timed out, and Weasel catches that per table and carries on, leaving the
        // row in another tenant's partition. Every stream's tenant and not just the session's, because
        // ForTenant puts others in the same save. #892: this is also where a tenant whose SPLIT failed
        // gets another attempt, for the same reason it can't happen any later. The start of a save usually
        // holds nothing a SPLIT could wait on, but a transaction open by then was begun by an exclusive
        // fetch, AppendExclusive or a read at a higher isolation level, and then a later session retries.
        if (_eventGraph.UseTenantPartitionedEvents)
        {
            foreach (var stream in _workTracker.Streams.Where(s => s.Events.Any()))
            {
                await _eventGraph.TenantSequences.ResolveAsync(stream.TenantId, HasOpenTransaction, token).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     #919: refuse what this unit of work would write under a string id or stream key that its
    ///     <c>varchar</c> column can't hold, before any of it is sent.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         At the save rather than in <c>Store</c>, which is synchronous and would have to block on a
    ///         round trip to find out the code page. The walk costs next to nothing, because an ASCII id
    ///         answers itself and only the first id outside it in a table has to ask.
    ///     </para>
    ///     <para>
    ///         A delete, an archive and a tombstone are dropped rather than refused. They're after a row
    ///         that can't exist, and a delete of a row that isn't there has never been an error, but they
    ///         can't be left to run: by the time the id reached the column it would be the id of a row
    ///         that does exist. The same goes for a read, which is why reads find nothing.
    ///     </para>
    /// </remarks>
    internal async Task AssertStringKeysFitAsync(CancellationToken token)
    {
        foreach (var operation in _workTracker.Operations)
        {
            if (operation is Operations.IStreamKeyedOperation { StreamId: string streamKey })
            {
                if (!await CouldHaveStreamAsync(streamKey, token).ConfigureAwait(false)) _workTracker.Remove(operation);
            }
            else if (operation is IStorageOperation { DocumentId: { } id and not (Guid or int or long) })
            {
                await assertDocumentIdFitsAsync(operation, id, token).ConfigureAwait(false);
            }
        }

        // #922: the doc_type of a hierarchy is the alias of the subclass written. The operation reports the
        // root type, so the alias is the document's own.
        foreach (var operation in _workTracker.Operations)
        {
            if (operation is not Operations.ClosedShapeOperationAdapter { Document: { } document }
                || operation.Role() == OperationRole.Deletion) continue;

            var mapping = _providers.GetProvider(operation.DocumentType).Mapping;
            if (!mapping.IsHierarchy() || document.GetType() == mapping.DocumentType) continue;

            var alias = mapping.AliasFor(document.GetType());
            if (await CodePageLosingAsync(mapping.QualifiedTableName, alias, token).ConfigureAwait(false) is { } aliasCodePage)
            {
                throw new Exceptions.ColumnValueOutsideCodePageException("doc_type", alias, aliasCodePage);
            }
        }

        var stringStreams = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsString;

        foreach (var stream in _workTracker.Streams)
        {
            // Only what will be written. A stream fetched for writing and left alone writes nothing.
            if (stream.Events.Count > 0 || stream.AlwaysEnforceConsistency)
            {
                if (stringStreams) await AssertStreamKeyFitsAsync(stream, token).ConfigureAwait(false);
            }

            foreach (var @event in stream.Events)
            {
                await assertEventColumnsFitAsync(@event, token).ConfigureAwait(false);
            }
        }
    }

    // #922: the other varchar columns an event writes. They're the events table's own columns, and a table
    // takes the database default when it's created, so the streams table's answer stands for them.
    private async Task assertEventColumnsFitAsync(IEvent @event, CancellationToken token)
    {
        // Only when the column is there to lose it: the session seeds a correlation id from Activity.Current
        if (Options.Events.EnableCorrelationId)
        {
            await assertColumnFitsAsync("correlation_id", @event.CorrelationId ?? CorrelationId, token).ConfigureAwait(false);
        }

        await assertColumnFitsAsync("dotnet_type", @event.DotNetTypeName, token).ConfigureAwait(false);

        if (@event.Tags is not { Count: > 0 }) return;

        foreach (var tag in @event.Tags)
        {
            var registration = _eventGraph.FindTagType(tag.TagType);
            if (registration == null || registration.SimpleType != typeof(string)) continue;

            await assertColumnFitsAsync($"tag value ({registration.TableSuffix})",
                JasperFx.Events.Tags.TagValueStringifier.Stringify(registration.ExtractValue(tag.Value)), token).ConfigureAwait(false);
        }
    }

    private async Task assertColumnFitsAsync(string column, string? value, CancellationToken token)
    {
        if (value == null || Ascii.IsValid(value)) return;

        if (await CodePageLosingAsync(_eventGraph.StreamsTableName, value, token).ConfigureAwait(false) is { } codePage)
        {
            throw new Exceptions.ColumnValueOutsideCodePageException(column, value, codePage);
        }
    }

    private async Task assertDocumentIdFitsAsync(Weasel.Storage.IStorageOperation operation, object id,
        CancellationToken token)
    {
        var mapping = _providers.GetProvider(operation.DocumentType).Mapping;
        if (mapping.StringKeyOf(id) is not { } key) return;

        if (await CodePageLosingAsync(mapping.QualifiedTableName, key, token).ConfigureAwait(false) is not { } codePage) return;

        if (operation.Role() == OperationRole.Deletion)
        {
            _workTracker.Remove(operation);
            return;
        }

        throw Exceptions.StringIdOutsideCodePageException.ForDocument(key, mapping.DocumentType,
            mapping.QualifiedTableName, codePage);
    }

    internal async Task AssertStreamKeyFitsAsync(StreamAction stream, CancellationToken token)
    {
        if (stream.Key is { } key
            && await CodePageLosingAsync(_eventGraph.StreamsTableName, key, token).ConfigureAwait(false) is { } codePage)
        {
            throw Exceptions.StringIdOutsideCodePageException.ForStream(key, stream.AggregateType,
                _eventGraph.StreamsTableName, codePage);
        }
    }

    /// <summary>
    ///     #864: provision a tenant ahead of a stream read whose locks outlast it, which the start of the
    ///     save is too late for. An exclusive read holds its lock from here to the commit, and so does any
    ///     read in a session that holds its transaction open at Serializable or Repeatable Read. A Snapshot
    ///     session takes no such lock, but DDL after its first read fails its next one.
    /// </summary>
    internal async Task ProvisionEventTenantBeforeHoldingLocksAsync(string tenantId, bool exclusive,
        CancellationToken token)
    {
        if (!_eventGraph.UseTenantPartitionedEvents) return;
        if (!exclusive && !_transactional.HoldsTransactionForSession) return;

        // Provisioning is what reads pc_tenant_partitions, so the schema has to be there first
        await EnsureEventStoreSchemaAsync(token).ConfigureAwait(false);
        await _eventGraph.TenantSequences.ResolveAsync(tenantId, HasOpenTransaction, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     #783 (marten#5688). Ends a transaction this session began before a save that then failed,
    ///     whatever the failure was.
    /// </summary>
    /// <remarks>
    ///     Never with the caller's token: when it is already cancelled, <c>DbTransaction.RollbackAsync</c>
    ///     returns a cancelled task <i>without</i> rolling back, which is the trap that left Marten's
    ///     transactions open. A rollback that itself fails must not replace the save's own exception, and
    ///     disposing the transaction is what gives its locks back either way.
    ///     <para>
    ///         #832: a transaction open at this point was begun before the save, so the caller was relying
    ///         on it. If the server has already ended it there is nothing to roll back, and the session
    ///         must not forget that it happened.
    ///     </para>
    ///     <para>
    ///         #854: nor when this rollback is what ends it, so that is recorded first, in case the
    ///         rollback fails.
    ///     </para>
    /// </remarks>
    private async Task abandonOpenTransactionAsync(Exception cause)
    {
        var tx = _transactional.Transaction;
        if (tx == null) return;

        _transactional.Transaction = null;
        try
        {
            if (tx.Connection is null)
            {
                _transactional.MarkSessionUnusable(cause);
            }
            else
            {
                _transactional.RefuseWrites(cause);
                await SqlTransactionRequests.RollbackAsync(tx).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            Logger.LogFailure("Failure trying to roll back after a failed SaveChangesAsync", e);
        }
        finally
        {
            await tx.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Notify the storage-agnostic <see cref="IEventStoreInstrumentation.AppendObserver" /> with the
    ///     events appended in the just-committed unit of work (polecat#213 / CritterWatch#500). Must be
    ///     called after commit but before the work tracker is reset. Best-effort: the events are already
    ///     durable, so an observer fault is logged rather than surfaced as a SaveChanges failure.
    /// </summary>
    private void NotifyAppendObserver()
    {
        var observer = Options.Events.AppendObserver;
        if (observer == null) return;

        var events = _workTracker.Streams.SelectMany(s => s.Events).ToList();
        if (events.Count == 0) return;

        try
        {
            observer(events);
        }
        catch (Exception ex)
        {
            Logger.LogFailure("IEventStoreInstrumentation.AppendObserver threw", ex);
        }
    }

    /// <summary>
    ///     #238: increment the opt-in <c>polecat.event.append</c> counter once per event committed in
    ///     this unit of work, tagged with the event type and tenant. Mirrors Marten's event-append
    ///     counter. Must be called after commit but before the work tracker is reset. No-op unless
    ///     <see cref="Polecat.Internal.OpenTelemetry.OpenTelemetryOptions.EventCountersEnabled" /> is on.
    /// </summary>
    private void RecordEventAppendMetrics()
    {
        var batchSize = OpenTelemetry.PolecatTracing.EventAppendBatchSize;
        if (batchSize.Enabled)
        {
            var total = 0L;
            foreach (var stream in _workTracker.Streams) total += stream.Events.Count;
            if (total > 0) batchSize.Record(total);
        }

        if (!Options.OpenTelemetry.EventCountersEnabled) return;

        var counter = Options.OpenTelemetry.EventAppendCounter;
        foreach (var stream in _workTracker.Streams)
        {
            foreach (var @event in stream.Events)
            {
                counter.Add(1,
                    new KeyValuePair<string, object?>("event_type", @event.EventTypeName),
                    new KeyValuePair<string, object?>("tenant_id", @event.TenantId ?? TenantId));
            }
        }
    }

    // ---- #273 closed-shape event append path -------------------------------------------------------
    // Routes appends through the shared Weasel.Storage.EventStorage<TId> hierarchy
    // (SqlServerEventStoreDialect + PolecatQuickAppendEventsOperation). Versions are assigned
    // client-side after a locking read so inline projections see them; sequences read back in Postprocess.

    private async Task ProcessStreamsClosedShapeAsync(CancellationToken token)
    {
        var storage = _eventGraph.ClosedShapeEventStorage;
        var isGuid = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid;

        var ops = new List<Weasel.Storage.IStorageOperation>();

        // First pass: run the no-event consistency asserts and collect the appending streams that
        // need a stream-state read, so the N per-stream locking round trips collapse into ONE
        // batched read per save below.
        var appendingStreams = new List<StreamAction>();
        foreach (var stream in _workTracker.Streams)
        {
            if (!stream.Events.Any())
            {
                if (stream.AlwaysEnforceConsistency && stream.ExpectedVersionOnServer.HasValue)
                {
                    // Reuse the bespoke assert: unlike the shared AssertStreamVersionOperation (which
                    // throws on any missing stream), Polecat treats a not-found stream with expected
                    // version 0 as consistent (0 == 0), matching FetchForWriting on a new stream.
                    await AssertStreamVersionAsync(stream, token).ConfigureAwait(false);
                }

                continue;
            }

            if (stream.ActionType != StreamActionType.Start)
            {
                appendingStreams.Add(stream);
            }
        }

        var states = await ReadStreamStatesForClosedShapeAsync(appendingStreams, token).ConfigureAwait(false);

        foreach (var stream in _workTracker.Streams)
        {
            if (!stream.Events.Any())
            {
                continue;
            }

            // Per-tenant partitioning: resolve (and lazily provision) the stream tenant's partition
            // ordinal + sequence once per stream before building the append op.
            var (partitionOrdinal, partitionSequenceName) = await ResolvePartitionForClosedShapeAsync(stream, token).ConfigureAwait(false);

            if (stream.ActionType == StreamActionType.Start)
            {
                AssignEventMetadataForClosedShape(stream, 0);
                stream.Version = stream.Events.Count;
                ops.AddRange(QuickAppendEventsClosedShape(storage, isGuid, stream,
                    Polecat.Events.Storage.StreamWriteMode.Insert, partitionOrdinal, partitionSequenceName));
            }
            else
            {
                var streamId = isGuid ? (object)stream.Id : stream.Key!;
                var (currentVersion, exists, archived) = states.TryGetValue((TenantOf(stream), streamId), out var state)
                    ? state
                    : (0L, false, false);

                if (archived)
                {
                    throw new JasperFx.Events.ArchivedStreamException(streamId);
                }

                if (stream.ExpectedVersionOnServer.HasValue && currentVersion != stream.ExpectedVersionOnServer.Value)
                {
                    throw new EventStreamUnexpectedMaxEventIdException(streamId, stream.AggregateType,
                        stream.ExpectedVersionOnServer.Value, currentVersion);
                }

                AssignEventMetadataForClosedShape(stream, currentVersion);
                stream.Version = currentVersion + stream.Events.Count;

                var mode = exists
                    ? Polecat.Events.Storage.StreamWriteMode.Update
                    : Polecat.Events.Storage.StreamWriteMode.Insert;
                if (exists) stream.ExpectedVersionOnServer ??= currentVersion;
                ops.AddRange(QuickAppendEventsClosedShape(storage, isGuid, stream, mode,
                    partitionOrdinal, partitionSequenceName));
            }
        }

        if (ops.Count == 0) return;

        await ExecuteClosedShapeEventOperationsAsync(ops, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     #420: build the append operations for the <see cref="StreamAction" />s a projection
    ///     raised, so the async daemon batch can write them inside its own transaction rather than
    ///     dropping them. Same path as the inline append — resolve the tenant partition, read the
    ///     stream rows under <c>UPDLOCK, HOLDLOCK</c> (one batched read for the whole group),
    ///     assign versions client-side from what that read returned, then build the closed-shape
    ///     quick-append ops.
    ///     <para>
    ///     The one deliberate difference is the concurrency expectation. JasperFx's
    ///     <c>EventSlice.BuildOperations</c> pre-assigns versions on the single-stream-start path
    ///     from the slice's own event count, which is the stream's real version only when the
    ///     projection has seen every event on that stream — so that guess is discarded and the
    ///     version this session just read under lock is used instead, both to number the events
    ///     and as the guard on the stream-row update.
    ///     </para>
    /// </summary>
    internal async Task<List<Weasel.Storage.IStorageOperation>> BuildRaisedEventAppendsAsync(
        IReadOnlyList<StreamAction> streams, CancellationToken token)
    {
        var ops = new List<Weasel.Storage.IStorageOperation>();
        if (streams.Count == 0) return ops;

        var storage = _eventGraph.ClosedShapeEventStorage;
        var isGuid = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid;

        foreach (var stream in streams)
        {
            await AssertStreamKeyFitsAsync(stream, token).ConfigureAwait(false); // #919
        }

        // One batched locking read for the whole group instead of one round trip per action.
        var states = await ReadStreamStatesForClosedShapeAsync(streams, token).ConfigureAwait(false);

        foreach (var stream in streams)
        {
            var (partitionOrdinal, partitionSequenceName) = await ResolvePartitionForClosedShapeAsync(stream, token).ConfigureAwait(false);

            var streamId = isGuid ? (object)stream.Id : stream.Key!;
            var (currentVersion, exists, archived) = states.TryGetValue((TenantOf(stream), streamId), out var state)
                ? state
                : (0L, false, false);

            if (archived)
            {
                throw new JasperFx.Events.ArchivedStreamException(streamId);
            }

            AssignEventMetadataForClosedShape(stream, currentVersion);
            stream.Version = currentVersion + stream.Events.Count;

            // Replace (not ??=) JasperFx's client-side guess with the locked read, so the guarded
            // stream-row update asserts something true rather than failing the shard on a stream the
            // projection has only partially seen. A stream row that does not exist yet is inserted.
            stream.ExpectedVersionOnServer = exists ? currentVersion : null;

            var mode = exists
                ? Polecat.Events.Storage.StreamWriteMode.Update
                : Polecat.Events.Storage.StreamWriteMode.Insert;

            ops.AddRange(QuickAppendEventsClosedShape(storage, isGuid, stream, mode,
                partitionOrdinal, partitionSequenceName));
        }

        return ops;
    }

    private async Task<(int? ordinal, string? sequenceName)> ResolvePartitionForClosedShapeAsync(
        StreamAction stream, CancellationToken token)
    {
        if (!_eventGraph.UseTenantPartitionedEvents) return (null, null);

        // Past the locking read by now, so a tenant whose SPLIT failed has to wait for the next save (#892)
        var storage = await _eventGraph.TenantSequences.ResolveAsync(stream.TenantId, insideTransaction: true, token).ConfigureAwait(false);
        return (storage.Ordinal, storage.SequenceName);
    }

    // #834: more than one operation when the stream is too big for one command, in the order they
    // have to run. Both append paths come through here, which is what covers the daemon's raised events.
    private static IReadOnlyList<Polecat.Events.Storage.PolecatQuickAppendEventsOperation> QuickAppendEventsClosedShape(
        object storage, bool isGuid, StreamAction stream, Polecat.Events.Storage.StreamWriteMode mode,
        int? partitionOrdinal, string? partitionSequenceName)
    {
        var op = (Polecat.Events.Storage.PolecatQuickAppendEventsOperation)(isGuid
            ? ((Weasel.Storage.EventStorage<Guid>)storage).QuickAppendEvents(stream)
            : ((Weasel.Storage.EventStorage<string>)storage).QuickAppendEvents(stream));
        op.Mode = mode;
        op.PartitionOrdinal = partitionOrdinal;
        op.PartitionSequenceName = partitionSequenceName;
        return op.SplitToFit(Polecat.Linq.SqlGeneration.JsonValueList.Budget);
    }

    private void AssignEventMetadataForClosedShape(StreamAction stream, long baseVersion)
    {
        var events = stream.Events;
        for (var i = 0; i < events.Count; i++)
        {
            var e = events[i];
            e.Version = baseVersion + i + 1;
            if (e.Id == Guid.Empty) e.Id = Guid.NewGuid();
            e.Timestamp = DateTimeOffset.UtcNow;
            e.TenantId = stream.TenantId;
            e.StreamId = stream.Id;
            e.StreamKey = stream.Key;

            if (CorrelationId != null && e.CorrelationId == null) e.CorrelationId = CorrelationId;
            if (CausationId != null && e.CausationId == null) e.CausationId = CausationId;
            if (LastModifiedBy != null && e.UserName == null) e.UserName = LastModifiedBy;

            if (Headers is { Count: > 0 })
            {
                e.Headers ??= new Dictionary<string, object>();
                foreach (var header in Headers)
                {
                    e.Headers.TryAdd(header.Key, header.Value);
                }
            }
        }
    }

    /// <summary>
    ///     Read the (version, exists, archived) state of every stream in <paramref name="streams" />
    ///     under <c>UPDLOCK, HOLDLOCK</c> in ONE round trip, instead of one locking read per stream.
    ///     A stream absent from the result simply has no entry in the returned dictionary — callers
    ///     treat that as (0, not-exists, not-archived), exactly as the old per-stream read did.
    ///     <para>
    ///     Lock ordering: the ids travel to <c>OPENJSON</c> pre-sorted — <see cref="SqlGuid" />
    ///     ordering for Guid identity (matching SQL Server's uniqueidentifier index order) and
    ///     ordinal ordering for string identity — so two sessions appending to overlapping stream
    ///     sets tend to acquire their row locks in a consistent order. A single statement per save
    ///     also removes the interleaving window the old N sequential statements offered.
    ///     </para>
    ///     <para>
    ///     Tenancy: each stream is read under its own tenant, which is not always the session's, because
    ///     <c>ForTenant</c> puts other tenants' streams in the same save (#892). A stream id is only unique
    ///     within a tenant, so the result is keyed by both. That costs one statement per tenant, and a
    ///     save for one tenant is still the one round trip.
    ///     </para>
    /// </summary>
    private async Task<Dictionary<(string tenantId, object streamId), (long currentVersion, bool exists, bool archived)>>
        ReadStreamStatesForClosedShapeAsync(IReadOnlyList<StreamAction> streams, CancellationToken token)
    {
        var states = new Dictionary<(string tenantId, object streamId), (long currentVersion, bool exists, bool archived)>();

        foreach (var tenant in streams.GroupBy(TenantOf).OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            await readStreamStatesForTenantAsync(tenant.Key, tenant.ToList(), states, token).ConfigureAwait(false);
        }

        return states;
    }

    private string TenantOf(StreamAction stream) => stream.TenantId ?? TenantId;

    private async Task readStreamStatesForTenantAsync(string tenantId, IReadOnlyList<StreamAction> streams,
        Dictionary<(string tenantId, object streamId), (long currentVersion, bool exists, bool archived)> states,
        CancellationToken token)
    {
        var isGuid = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid;

        if (streams.Count == 1)
        {
            // Single-aggregate saves are the hot path; keep the simple point-lookup SQL (and its plan).
            var streamId = isGuid ? (object)streams[0].Id : streams[0].Key!;

            await using var cmd = new SqlCommand();
            cmd.CommandText = _eventGraph.LockedStreamReadSql("version, is_archived",
                "id = @id AND tenant_id = @tenant_id", TenantId, cmd);
            cmd.Parameters.AddIdParameter("@id", streamId);
            cmd.Parameters.AddVarChar("@tenant_id", tenantId);

            // Through the db-neutral seam so the session logger sees the stream-state read
            await using var reader = (await ExecuteReaderAsync((System.Data.Common.DbCommand)cmd, token).ConfigureAwait(false));
            if (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                states[(tenantId, streamId)] = (reader.GetInt64(0), true, reader.GetBoolean(1));
            }

            return;
        }

        // #363 OPENJSON id-list pattern (see PolecatDocumentStorage): SQL Server has no array
        // parameters, so the ids travel as one JSON array. OPENJSON's value column is nvarchar(4000),
        // so cast back to the id column's own type to keep the index seek.
        object[] ids = isGuid
            ? streams.Select(x => x.Id).Distinct()
                .OrderBy(x => new System.Data.SqlTypes.SqlGuid(x))
                .Cast<object>().ToArray()
            : streams.Select(x => x.Key!).Distinct()
                .OrderBy(x => x, StringComparer.Ordinal)
                .Cast<object>().ToArray();

        var idsSelect = isGuid
            ? "SELECT CAST(value AS uniqueidentifier) AS id FROM OPENJSON(@ids)"
            : "SELECT CAST(value AS varchar(250)) AS id FROM OPENJSON(@ids)";

        await using var batchCmd = new SqlCommand();
        batchCmd.CommandText = _eventGraph.LockedStreamReadSql("s.id, s.version, s.is_archived",
            "s.tenant_id = @tenant_id", TenantId, batchCmd, idsSelect);
        batchCmd.Parameters.Add(
            new SqlParameter("@ids", WriteStreamIdsAsJsonArray(ids))
            {
                SqlDbType = System.Data.SqlDbType.NVarChar
            });
        batchCmd.Parameters.AddVarChar("@tenant_id", tenantId);

        // Through the db-neutral seam so the session logger sees the stream-state read
        await using var batchReader = (await ExecuteReaderAsync((System.Data.Common.DbCommand)batchCmd, token).ConfigureAwait(false));
        while (await batchReader.ReadAsync(token).ConfigureAwait(false))
        {
            var id = isGuid ? (object)batchReader.GetGuid(0) : batchReader.GetString(0);
            states[(tenantId, id)] = (batchReader.GetInt64(1), true, batchReader.GetBoolean(2));
        }
    }

    /// <summary>
    ///     Stream ids as one JSON array string for OPENJSON — the same manual, reflection-free
    ///     writing as <c>SqlServerStorageDialect.WriteIdsAsJsonArray</c> (that one is private on a
    ///     generic type). Only Guid and string, the two stream identity modes.
    /// </summary>
    private static string WriteStreamIdsAsJsonArray(object[] ids)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        using (var writer = new System.Text.Json.Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var id in ids)
            {
                switch (id)
                {
                    case Guid guid:
                        writer.WriteStringValue(guid);
                        break;
                    case string text:
                        writer.WriteStringValue(text);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(ids),
                            $"Unsupported stream id type {id?.GetType().FullName ?? "null"}; expected Guid or string.");
                }
            }

            writer.WriteEndArray();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    internal async Task ExecuteClosedShapeEventOperationsAsync(
        List<Weasel.Storage.IStorageOperation> operations, CancellationToken token)
    {
        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);

        for (var i = 0; i < operations.Count; i++)
        {
            if (i > 0) builder.StartNewCommand();
            Operations.StorageOperationExecution.Configure(operations[i], builder, (Weasel.Storage.IStorageSession)this);
        }

        builder.Compile();

        var exceptions = new List<Exception>();
        try
        {
            await using var reader = (await ExecuteReaderAsync(batch, token).ConfigureAwait(false));
            await OperationResultSets.ApplyAsync(reader, operations, exceptions, token).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            if (BatchExceptionTransforms.TryTransform(ex, batch, operations, Options.ExceptionTransforms,
                    out var transformed))
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(transformed!).Throw();
            }

            throw;
        }

        if (exceptions.Count == 1)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exceptions[0]).Throw();
        }

        if (exceptions.Count > 0)
        {
            throw new AggregateException(exceptions);
        }
    }

    private async Task AssertStreamVersionAsync(StreamAction stream, CancellationToken token)
    {
        var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        await using var cmd = new SqlCommand();
        cmd.CommandText =
            $"SELECT version FROM {_eventGraph.StreamsTableName} WHERE id = @id AND tenant_id = @tenant_id;";
        cmd.Parameters.AddIdParameter("@id", streamId);
        cmd.Parameters.AddVarChar("@tenant_id", TenantOf(stream));

        await using var reader = (await ExecuteReaderAsync(cmd, token).ConfigureAwait(false));
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            // Stream doesn't exist — consistent only if expected version is 0
            if (stream.ExpectedVersionOnServer!.Value != 0)
            {
                throw new EventStreamUnexpectedMaxEventIdException(streamId, stream.AggregateType,
                    stream.ExpectedVersionOnServer.Value, 0);
            }
            return;
        }

        var actualVersion = reader.GetInt64(0);
        if (actualVersion != stream.ExpectedVersionOnServer!.Value)
        {
            throw new EventStreamUnexpectedMaxEventIdException(streamId, stream.AggregateType,
                stream.ExpectedVersionOnServer.Value, actualVersion);
        }
    }

    // IStorageOperations
    public bool EnableSideEffectsOnInlineProjections => _eventGraph.EnableSideEffectsOnInlineProjections;

    private IMessageBatch? _inlineMessageBatch;

    public async ValueTask<IMessageSink> GetOrStartMessageSink()
    {
        if (_inlineMessageBatch is not null) return _inlineMessageBatch;

        _inlineMessageBatch = await Options.Events.MessageOutbox
            .CreateBatch(this)
            .ConfigureAwait(false);

        return _inlineMessageBatch;
    }

    Task<IProjectionStorage<TDoc, TId>> IStorageOperations.FetchProjectionStorageAsync<TDoc, TId>(
        string tenantId, CancellationToken cancellationToken)
    {
        // Check for custom projection storage providers (e.g., EF Core)
        if (Options.CustomProjectionStorageProviders.TryGetValue(typeof(TDoc), out var factory))
        {
            return Task.FromResult((IProjectionStorage<TDoc, TId>)factory(this, tenantId));
        }

        var provider = _providers.GetProvider(typeof(TDoc));
        IProjectionStorage<TDoc, TId> storage =
#pragma warning disable CS8714 // notnull constraint mismatch with JasperFx interface
            new PolecatProjectionStorage<TDoc, TId>(this, provider, tenantId);
#pragma warning restore CS8714
        return Task.FromResult(storage);
    }


    public void Eject<T>(T document) where T : notnull
    {
        var provider = _providers.GetProvider<T>();
        var id = provider.Mapping.GetId(document);
        _workTracker.EjectDocument(typeof(T), id);
        OnDocumentEjected(typeof(T), id);
        // #273 E2b: the closed-shape load path caches in the shared ItemMap / version tracker.
        if (provider.Mapping.DocumentType == typeof(T))
        {
            var session = (Weasel.Storage.IStorageSession)this;
            session.StorageFor<T>().EjectById(session, id);
        }
    }

    public void EjectAllOfType(Type type)
    {
        _workTracker.EjectAllOfType(type);
        OnAllOfTypeEjected(type);
        // #273 E2b: drop the closed-shape shared-ItemMap cache for the type as well.
        ((Weasel.Storage.IStorageSession)this).ItemMap.Remove(type);
    }

    public void EjectAllPendingChanges()
    {
        _workTracker.Reset();
    }

    /// <summary>
    ///     Called when a document is queued via Store/Insert/Update.
    ///     Override in IdentityMap session to track in the map.
    /// </summary>
    protected virtual void OnDocumentStored(Type documentType, object id, object document)
    {
        // No-op in lightweight session
    }

    /// <summary>
    ///     Called when a document is ejected. Override in IdentityMap session
    ///     to remove from the identity map.
    /// </summary>
    protected virtual void OnDocumentEjected(Type documentType, object id)
    {
        // No-op in lightweight session
    }

    /// <summary>
    ///     Called when all documents of a type are ejected. Override in IdentityMap session
    ///     to clear the identity map for that type.
    /// </summary>
    protected virtual void OnAllOfTypeEjected(Type documentType)
    {
        // No-op in lightweight session
    }

    // #273 phase E1: document sessions resolve the Lightweight closed-shape storage flavor.
    internal override Weasel.Storage.IDocumentStorage<T> SelectClosedShapeStorage<T>(
        Weasel.Storage.DocumentProvider<T> provider)
        => provider.Lightweight;

    private void SyncMetadata(object document)
    {
        if (document is ITracked tracked)
        {
            tracked.CorrelationId = CorrelationId;
            tracked.CausationId = CausationId;
            tracked.LastModifiedBy = LastModifiedBy;
        }

        if (document is Metadata.ITenanted tenanted)
        {
            tenanted.TenantId = TenantId;
        }
    }
}
