using System.Data.Common;
using JasperFx.Events;
using Polecat.Internal;
using Polecat.Linq.SqlGeneration;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Protected;

internal class DeleteEventsOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly EventGraph _events;
    private readonly long[] _sequences;
    private readonly string _tenantId;

    public DeleteEventsOperation(EventGraph events, long[] sequences, string tenantId)
    {
        _events = events;
        _sequences = sequences;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // #841: no foreign key under archived partitioning, so the compacted events' tag rows go here.
        // Keyed directly on seq_id, so they can go after their events.
        var tagTables = _events.TagRowsCascadeWithEvents
            ? []
            : _events.TagTypes.Select(_events.TagTableName).ToArray();

        // #834: compaction exists for the streams that have grown long, so this list is at its largest
        // on the very streams it is for, and one parameter per sequence refused any stream past about
        // 2100 folded events. The same decision #710 made for a LINQ IN list. Every statement below
        // carries the whole list, so the decision is made for all of them together.
        var asJsonArray = JsonValueList.ShouldBindAsJsonArray(builder, _sequences.Length * (1 + tagTables.Length));

        AppendDelete(builder, _events.EventsTableName, asJsonArray);

        foreach (var tagTable in tagTables)
        {
            AppendDelete(builder, tagTable, asJsonArray);
        }
    }

    private void AppendDelete(ICommandBuilder builder, string table, bool asJsonArray)
    {
        builder.Append($"DELETE FROM {table} WHERE ");

        if (asJsonArray)
        {
            JsonValueList.AppendInClause(builder, "seq_id", _sequences, "bigint", value => value);
        }
        else
        {
            builder.Append("seq_id IN (");
            for (var i = 0; i < _sequences.Length; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.AppendParameter(_sequences[i]);
            }

            builder.Append(")");
        }

        // marten#5234's Polecat twin: under UseTenantPartitionedEvents seq_id is per-tenant, so
        // without this predicate compacting one tenant's stream permanently deletes the
        // same-numbered events out of every other tenant's partition.
        builder.AppendConjoinedTenantFilter(_events, _tenantId);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
