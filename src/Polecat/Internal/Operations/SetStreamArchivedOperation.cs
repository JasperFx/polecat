using System.Data.Common;
using Polecat.Events;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     Flips <c>is_archived</c> on a stream and all of its events. Archive and un-archive differ only
///     in the flag value, so both ride this one operation (polecat#318).
/// </summary>
internal class SetStreamArchivedOperation : IStorageOperation
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;
    private readonly bool _archived;

    public SetStreamArchivedOperation(EventGraph events, object streamId, string tenantId, bool archived)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
        _archived = archived;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // A literal rather than a parameter so SQL Server can still eliminate partitions at compile
        // time where is_archived participates in the partition scheme.
        var flag = _archived ? 1 : 0;

        builder.Append($"""
            UPDATE {_events.StreamsTableName} SET is_archived = {flag}
            WHERE id = @id AND tenant_id = @tenant_id;
            UPDATE {_events.EventsTableName} SET is_archived = {flag}
            WHERE stream_id = @id AND tenant_id = @tenant_id;
            """);
        builder.AddParameters(new Dictionary<string, object?> { ["id"] = _streamId, ["tenant_id"] = _tenantId });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
