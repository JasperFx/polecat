using System.Data.Common;
using Weasel.Core;
using Weasel.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     Adapts a shared closed-shape write operation (Weasel.Storage 9.13.0) to Polecat's
///     unit-of-work operation currency (#273 E2b). The bespoke pipeline's WorkTracker,
///     ChangeSet, and eject bookkeeping speak <see cref="Polecat.Internal.IStorageOperation" />
///     (DocumentType/DocumentId/Role + one-arg ConfigureCommand); the shared operations speak
///     the two-arg shared contract. The adapter captures the session at creation so the one-arg
///     call can forward, and surfaces Document/DocumentId for change-set/eject semantics.
///     Retires with the bespoke currency in E2e.
/// </summary>
internal sealed class ClosedShapeOperationAdapter : IStorageOperation, IDocumentStorageOperation
{
    private readonly Weasel.Storage.IStorageOperation _inner;
    private readonly IStorageSession _session;

    public ClosedShapeOperationAdapter(Weasel.Storage.IStorageOperation inner, IStorageSession session,
        object document, object? documentId)
    {
        _inner = inner;
        _session = session;
        Document = document;
        DocumentId = documentId;
    }

    public object Document { get; }
    public object? DocumentId { get; }
    public Type DocumentType => _inner.DocumentType;

    public OperationRole Role() => _inner.Role();

    public void ConfigureCommand(Weasel.SqlServer.ICommandBuilder builder)
        => _inner.ConfigureCommand(builder, _session);

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => _inner.PostprocessAsync(reader, exceptions, token);
}
