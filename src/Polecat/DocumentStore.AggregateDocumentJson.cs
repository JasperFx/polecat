using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.ExceptionServices;
using JasperFx.Core.Reflection;
using JasperFx.Documents;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;

namespace Polecat;

/// <summary>
///     jasperfx#1082 (CritterWatch#1314): a document rebuilt NOW from its stream, through the single-stream
///     projection registered for its type, as the JSON the store would write for it — so a console can diff
///     it against the stored row and answer "is this read model stale or wrong?".
/// </summary>
/// <remarks>
///     <para>
///         <b>The aggregation is the store's own live aggregation.</b> <c>AggregateStreamAsync&lt;T&gt;</c>
///         resolves its aggregator through <c>Projections.AggregatorFor&lt;T&gt;()</c>, which hands back the
///         registered projection itself, so the projection's own Create/Apply/ShouldDelete methods and stream
///         identity are what run. Nothing is written: a query session reads the events and nothing else.
///     </para>
///     <para>
///         <b>The JSON is the store's own rendering.</b> <c>Options.Serializer.ToJson</c> is the call the
///         document storage makes when it binds an upsert's <c>data</c> parameter (and the one
///         <c>PreviewDocumentJsonAsync</c> shares), so casing, enum storage and custom converters all match
///         <see cref="StoredDocument.Json" />.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026",
    Justification = "jasperfx#1082: closes AggregateStreamAsync<T> over a registered projection's aggregate type and serializes through ISerializer; the aggregate types are the store's registered projection documents, which Polecat's projection and storage paths already require to be preserved. The explicit IDocumentStoreDiagnostics implementation cannot carry the annotation itself (IL2046).")]
[UnconditionalSuppressMessage("AOT", "IL3050",
    Justification = "jasperfx#1082: MakeGenericMethod over a registered aggregate type. An AOT consumer aggregates through the strong-typed AggregateStreamAsync<T>. The explicit IDocumentStoreDiagnostics implementation cannot carry the annotation itself (IL3051).")]
[UnconditionalSuppressMessage("Trimming", "IL2060",
    Justification = "jasperfx#1082: see the IL2026 justification.")]
public partial class DocumentStore
{
    private static MethodInfo? _aggregateForDiagnostics;

    async Task<string?> IDocumentStoreDiagnostics.AggregateDocumentJsonAsync(string documentTypeName, string id,
        string? tenantId, CancellationToken token)
    {
        var resolved = ResolveForDiagnostics(documentTypeName)
                       ?? throw new ArgumentException(
                           $"Unknown document type '{documentTypeName}'. It is not a document type this Polecat store maps.",
                           nameof(documentTypeName));

        var documentType = resolved.RequestedType;
        if (SingleStreamProjectionFor(documentType) == null)
        {
            throw new InvalidOperationException(
                $"'{documentType.FullNameInCode()}' is not produced by a single-stream projection registered on this " +
                "store, so no one stream is its source and it cannot be rebuilt from one. Only a type with " +
                $"{nameof(DocumentTypeRef)}.{nameof(DocumentTypeRef.SingleStreamProjection)} set can be.");
        }

        object streamId;
        if (Events.StreamIdentity == StreamIdentity.AsGuid)
        {
            // An id that cannot be a stream id names no stream, which is the "no stream" answer.
            if (!Guid.TryParse(id, out var guid)) return null;
            streamId = guid;
        }
        else
        {
            if (string.IsNullOrEmpty(id)) return null;
            streamId = id;
        }

        await using var session = DiagnosticsAggregationSession(DocumentQueryOptions.NormalizeTenantId(tenantId));

        _aggregateForDiagnostics ??= typeof(DocumentStore).GetMethod(nameof(AggregateForDiagnosticsAsync),
            BindingFlags.NonPublic | BindingFlags.Static)!;

        object? aggregate;
        try
        {
            var task = (Task<object?>)_aggregateForDiagnostics.MakeGenericMethod(documentType)
                .Invoke(null, [session, streamId, token])!;
            aggregate = await task.ConfigureAwait(false);
        }
        catch (TargetInvocationException e) when (e.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            throw;
        }

        // No stream, or a projection that deleted the document: nothing would be stored.
        return aggregate == null ? null : Options.Serializer.ToJson(aggregate);
    }

    private static async Task<object?> AggregateForDiagnosticsAsync<T>(IQuerySession session, object streamId,
        CancellationToken token) where T : class
    {
        return streamId is Guid guid
            ? await session.Events.AggregateStreamAsync<T>(guid, token: token).ConfigureAwait(false)
            : await session.Events.AggregateStreamAsync<T>((string)streamId, token: token).ConfigureAwait(false);
    }

    /// <summary>
    ///     The session the stream is read through. A named tenant routes as any session does; the default
    ///     tenant on a single-database store is bound to the database with
    ///     <see cref="SessionOptions.AllowAnyTenant" />, so a store with
    ///     <see cref="StoreOptions.DefaultTenantUsageEnabled" /> off answers "no stream" rather than refusing a
    ///     read-only diagnostic (#819 is the same trap on the explorer reads).
    /// </summary>
    private IQuerySession DiagnosticsAggregationSession(string? tenantId)
    {
        if (tenantId != null) return QuerySession(tenantId);

        return IsSingleDatabase
            ? QuerySession(SessionOptions.ForDatabase(SingleDatabase))
            : QuerySession();
    }

    /// <summary>
    ///     The registered single-stream projection that produces <paramref name="documentType" />, or null.
    ///     Top-level registrations only: a projection that exists only as a composite's member is not one
    ///     the contract offers a rebuild for.
    /// </summary>
    private JasperFx.Events.Projections.IProjectionSource<IDocumentSession, IQuerySession>? SingleStreamProjectionFor(
        Type documentType)
        => Options.Projections.All.FirstOrDefault(source =>
            source is IAggregateProjection { Scope: AggregationScope.SingleStream } aggregate
            && aggregate.AggregateType == documentType);
}
