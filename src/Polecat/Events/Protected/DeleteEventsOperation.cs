using System.Data.Common;
using JasperFx.Events;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Protected;

internal class DeleteEventsOperation : Polecat.Internal.IStorageOperation
{
    private readonly EventGraph _events;
    private readonly long[] _sequences;

    public DeleteEventsOperation(EventGraph events, long[] sequences)
    {
        _events = events;
        _sequences = sequences;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"DELETE FROM {_events.EventsTableName} WHERE seq_id IN (");
        for (var i = 0; i < _sequences.Length; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.AppendParameter(_sequences[i]);
        }
        builder.Append(");");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
