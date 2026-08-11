using System.Data.Common;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Projections;

/// <summary>
///     Retires every <c>pc_natural_key_{type}</c> row that maps a natural key to this stream
///     <em>other</em> than the one the accompanying upsert is about to write (polecat#435, the twin of
///     marten#5041/#5044).
/// </summary>
/// <remarks>
///     <para>
///         A stream has exactly one <em>current</em> natural key, but
///         <see cref="NaturalKeyUpsertOperation" /> only ever MERGEs. When an event changes the key,
///         the row carrying the previous value stays behind still pointing at the same stream, so
///         <c>FetchForWriting</c>-by-natural-key keeps resolving the superseded alias, the table
///         accumulates one dead row per rename, and — because <c>natural_key_value</c> is the primary
///         key — the retired value permanently squats on its slot so no other stream can ever claim it.
///     </para>
///     <para>
///         The delete is scoped to this stream (and tenant, when conjoined) so a key legitimately owned
///         by some <em>other</em> stream is never touched, and it is queued immediately ahead of the
///         upsert. <c>WorkTracker</c> preserves insertion order and the session flushes its operations
///         in that order, so a create-then-rename inside a single <c>SaveChangesAsync</c> still lands on
///         the newest value: each retire only ever clears values that differ from the one its own
///         upsert is about to write.
///     </para>
///     <para>
///         The rebuild path gets this for free — it shares
///         <c>NaturalKeyProjection.QueueOperationForEvent</c> with the inline append path — which
///         matters because a rebuild replays the same rename sequence and would otherwise reproduce
///         exactly the same set of dead rows after teardown.
///     </para>
/// </remarks>
internal class NaturalKeyRetireOperation : Polecat.Internal.IStorageOperation
{
    private readonly string _tableName;
    private readonly object _currentNaturalKeyValue;
    private readonly object _streamId;
    private readonly bool _isGuidStream;
    private readonly bool _isConjoined;
    private readonly string? _tenantId;

    public NaturalKeyRetireOperation(string tableName, object currentNaturalKeyValue, object streamId,
        bool isGuidStream, bool isConjoined = false, string? tenantId = null)
    {
        _tableName = tableName;
        _currentNaturalKeyValue = currentNaturalKeyValue;
        _streamId = streamId;
        _isGuidStream = isGuidStream;
        _isConjoined = isConjoined;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Deletion;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var streamColumn = _isGuidStream ? "stream_id" : "stream_key";

        builder.Append($"DELETE FROM {_tableName} WHERE {streamColumn} = ");
        // #363: a string stream key must bind varchar to seek the varchar(250) stream_key column.
        builder.AppendParameter(_streamId, _streamId is string ? System.Data.SqlDbType.VarChar : null);

        if (_isConjoined)
        {
            builder.Append(" AND tenant_id = ");
            builder.AppendParameter(_tenantId!, System.Data.SqlDbType.VarChar);
        }

        builder.Append(" AND natural_key_value <> ");
        builder.AppendParameter(_currentNaturalKeyValue);

        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
