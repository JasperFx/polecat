using System.Data.Common;
using Polecat.Events;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class TombstoneStreamOperation : IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;

    public TombstoneStreamOperation(EventGraph events, object streamId, string tenantId)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Deletion;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // #841: ahead of the events, because this finds the tag rows through them. Everywhere else the
        // foreign key does this, but archived partitioning has none and the rows would be orphaned.
        if (!_events.TagRowsCascadeWithEvents)
        {
            foreach (var registration in _events.TagTypes)
            {
                builder.Append($"""
                    DELETE t FROM {_events.TagTableName(registration)} t
                    INNER JOIN {_events.EventsTableName} e ON {_events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id;

                    """);
            }
        }

        builder.Append($"""
            DELETE FROM {_events.EventsTableName}
            WHERE stream_id = @id AND tenant_id = @tenant_id;
            DELETE FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id;
            """);
        builder.AddParameters(new Dictionary<string, object?> { ["id"] = _streamId, ["tenant_id"] = _tenantId });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
