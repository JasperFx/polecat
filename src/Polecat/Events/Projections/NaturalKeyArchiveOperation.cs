using System.Data.Common;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Projections;

/// <summary>
///     Storage operation that marks a natural key mapping as archived
///     when the corresponding stream is archived. For conjoined tenancy,
///     filters by tenant_id as well.
/// </summary>
internal class NaturalKeyArchiveOperation : Polecat.Internal.IStorageOperation
{
    private readonly string _tableName;
    private readonly object _streamId;
    private readonly bool _isGuidStream;
    private readonly bool _isConjoined;
    private readonly string? _tenantId;
    private readonly bool _archived;

    /// <param name="archived">
    ///     #556: false un-archives. The explicit UnArchiveStream API has to be able to put a key back
    ///     into service, or an un-archived stream would stay permanently unreachable by the
    ///     identifier it was created with -- the same silent unreachability #549 is about, reached
    ///     from the other side. The Archived-event path only ever passes true.
    /// </param>
    public NaturalKeyArchiveOperation(string tableName, object streamId, bool isGuidStream,
        bool isConjoined = false, string? tenantId = null, bool archived = true)
    {
        _tableName = tableName;
        _streamId = streamId;
        _isGuidStream = isGuidStream;
        _isConjoined = isConjoined;
        _tenantId = tenantId;
        _archived = archived;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var streamColumn = _isGuidStream ? "stream_id" : "stream_key";

        builder.Append($"UPDATE {_tableName} SET is_archived = {(_archived ? 1 : 0)} WHERE {streamColumn} = ");
        // #363: string stream keys must bind varchar to seek the varchar(250) stream_key column.
        builder.AppendParameter(_streamId, _streamId is string ? System.Data.SqlDbType.VarChar : null);

        if (_isConjoined)
        {
            builder.Append(" AND tenant_id = ");
            builder.AppendParameter(_tenantId!, System.Data.SqlDbType.VarChar);
        }

        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
