using System.Data.Common;
using Polecat.Events;
using Polecat.Events.Dcb;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     Flips <c>is_archived</c> on a stream and all of its events. Archive and un-archive differ only
///     in the flag value, so both ride this one operation (polecat#318).
/// </summary>
/// <remarks>
///     The flip also moves the <c>pc_dcb_tag_version</c> row of every tag on the stream's events (#841). A
///     tag read excludes archived events, so without that, a boundary that read a tag while the stream was
///     archived would save straight through an un-archive that brought the tag's events back: a decision
///     made without them. See <see cref="StreamTagVersionBump" />.
/// </remarks>
internal class SetStreamArchivedOperation : IStorageOperation, Weasel.Storage.NoDataReturnedCall
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

        StreamTagVersionBump.WriteSql(builder, _events);

        builder.AddParameters(new Dictionary<string, object?> { ["id"] = _streamId, ["tenant_id"] = _tenantId });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
