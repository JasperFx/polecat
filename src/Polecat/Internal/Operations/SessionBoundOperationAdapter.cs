using System.Data.Common;
using Weasel.Core;
using Weasel.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     Binds a shared closed-shape operation to the session that must execute it, for pipelines
///     that configure commands without a session of their own — specifically the async daemon
///     batch, which calls <see cref="StorageOperationExecution.Configure" /> with a null session
///     because every operation it carries is expected to be fully bound already.
///     <para>
///     Deliberately NOT an <see cref="IDocumentStorageOperation" />, unlike
///     <see cref="ClosedShapeOperationAdapter" />: the operations wrapped here write events, not
///     documents. Exposing a <c>Document</c> would put them in the batch's
///     <see cref="Polecat.Services.IChangeSet" /> <c>Updated</c>/<c>Inserted</c> sets and hand
///     <c>IChangeListener</c> implementations a <c>StreamAction</c> where they expect a document.
///     Raised events reach listeners through the change set's stream collection instead.
///     </para>
/// </summary>
internal sealed class SessionBoundOperationAdapter : IStorageOperation
{
    private readonly Weasel.Storage.IStorageOperation _inner;
    private readonly IStorageSession _session;

    public SessionBoundOperationAdapter(Weasel.Storage.IStorageOperation inner, IStorageSession session)
    {
        _inner = inner;
        _session = session;
    }

    public Type DocumentType => _inner.DocumentType;

    public OperationRole Role() => _inner.Role();

    public void ConfigureCommand(Weasel.SqlServer.ICommandBuilder builder)
        => _inner.ConfigureCommand(builder, _session);

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => _inner.PostprocessAsync(reader, exceptions, token);
}
