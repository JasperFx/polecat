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
///     <para>
///         The flip also moves the <c>pc_dcb_tag_version</c> row of every tag on the stream's events
///         (#841). A tag read excludes archived events, so archiving or un-archiving a stream changes
///         what a boundary over its tags reads, and the boundary check only ever compares that counter.
///         Left alone, a boundary that read a tag while the stream was archived would save straight
///         through an un-archive that brought the tag's events back: a decision made without them, and
///         precisely the conflict the check exists to report. The same save that reads the counter
///         again sees the current events, so the cost of this is a retry and never a wrong answer.
///     </para>
///     <para>
///         <c>HOLDLOCK</c> because this is a set-based create-or-bump, where
///         <c>DcbTagVersionPhase</c> can insert one row and catch the duplicate. It takes the range
///         locks that phase avoids, which it can afford for the same reason it is a separate statement
///         rather than part of the phase: archiving is rare and appending is not.
///     </para>
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

        foreach (var registration in _events.TagTypes)
        {
            var tagValue = TagValueStringifier.ToSql(registration.SimpleType, "t.value");

            // CONVERT because a dictionary parameter binds as nvarchar, and comparing that to the
            // varchar key of pc_dcb_tag_version would convert the column and range-lock a scan of it.
            builder.Append($"""

                MERGE {_events.DcbTagVersionTableName} WITH (HOLDLOCK) AS target
                USING (
                    SELECT DISTINCT '{SqlEscaping.LiteralBody(registration.TableSuffix)}' AS tag_table,
                        {tagValue} AS tag_value, CONVERT(varchar(250), @tenant_id) AS tenant_id
                    FROM {_events.TagTableName(registration)} t
                    INNER JOIN {_events.EventsTableName} e ON {_events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id
                ) AS source
                ON target.tag_table = source.tag_table AND target.tag_value = source.tag_value
                    AND target.tenant_id = source.tenant_id
                WHEN MATCHED THEN UPDATE SET version = target.version + 1
                WHEN NOT MATCHED THEN INSERT (tag_table, tag_value, tenant_id, version)
                    VALUES (source.tag_table, source.tag_value, source.tenant_id, 1);
                """);
        }

        builder.AddParameters(new Dictionary<string, object?> { ["id"] = _streamId, ["tenant_id"] = _tenantId });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
