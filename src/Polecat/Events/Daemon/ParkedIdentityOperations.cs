using System.Buffers;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Text.Json;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Events.Schema;
using Weasel.Core;

namespace Polecat.Events.Daemon;

/// <summary>
///     marten#6003 / #1053: write (insert or replace) the parked row of an identity that failed in a daemon batch, so
///     it commits with the batch's progression or not at all.
/// </summary>
internal sealed class ParkIdentityOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly string _table;
    private readonly ParkedIdentity _parked;
    private readonly bool _unicode;

    public ParkIdentityOperation(string table, ParkedIdentity parked, bool unicode)
    {
        _table = table;
        _parked = parked;
        _unicode = unicode;
    }

    public Type DocumentType => typeof(object);

    public OperationRole Role() => OperationRole.Upsert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var key = KeyColumns(_unicode);
        var p = _parked;

        // HOLDLOCK: two batches of one shard never run at once, but a replay request written from another process
        // touches the same row
        builder.Append($"MERGE {_table} WITH (UPDLOCK, HOLDLOCK) AS t USING (SELECT ");
        builder.AppendParameter(p.ProjectionName, key);
        builder.Append(" AS projection_name, ");
        builder.AppendParameter((int)p.ProjectionVersion, SqlDbType.Int);
        builder.Append(" AS projection_version, ");
        builder.AppendParameter(p.ShardKey, key);
        builder.Append(" AS shard_key, ");
        builder.AppendParameter(p.TenantId, key);
        builder.Append(" AS tenant_id, ");
        builder.AppendParameter(p.IdentityText, key);
        builder.Append(" AS identity_text) AS s ON t.projection_name = s.projection_name AND t.projection_version = s.projection_version AND t.shard_key = s.shard_key AND t.tenant_id = s.tenant_id AND t.identity_text = s.identity_text ");

        // The values, once, as a derived row
        var names = new[]
        {
            "identity_type", "first_failing_seq", "resume_after", "failure_category", "exception_type",
            "exception_message", "event_type", "event_id", "failing_stream", "parked_at", "replay_attempts",
            "last_attempt_at", "last_attempt_error", "requested_action", "requested_at"
        };

        builder.Append("WHEN MATCHED THEN UPDATE SET identity_type = ");
        appendValues(builder, ", {0} = ", names);
        builder.Append(" WHEN NOT MATCHED THEN INSERT (projection_name, projection_version, shard_key, tenant_id, identity_text, ");
        builder.Append(string.Join(", ", names));
        builder.Append(") VALUES (s.projection_name, s.projection_version, s.shard_key, s.tenant_id, s.identity_text, ");
        appendValues(builder, ", ", names);
        builder.Append(");");
    }

    // The fifteen values, in the order of the names, joined by the separator (which may name the column)
    private void appendValues(ICommandBuilder builder, string separator, string[] names)
    {
        var p = _parked;
        for (var i = 0; i < names.Length; i++)
        {
            if (i > 0) builder.Append(string.Format(CultureInfo.InvariantCulture, separator, names[i]));

            switch (names[i])
            {
                case "identity_type":
                    builder.AppendParameter(p.IdentityType, SqlDbType.VarChar);
                    break;
                case "first_failing_seq":
                    builder.AppendParameter(p.FirstFailingSequence, SqlDbType.BigInt);
                    break;
                case "resume_after":
                    builder.AppendParameter(p.ResumeAfter, SqlDbType.BigInt);
                    break;
                case "failure_category":
                    builder.AppendParameter(p.Category.ToString(), SqlDbType.VarChar);
                    break;
                case "exception_type":
                    builder.AppendParameter((object?)Truncate(p.ExceptionType, 500), SqlDbType.NVarChar);
                    break;
                case "exception_message":
                    builder.AppendParameter((object?)p.ExceptionMessage, SqlDbType.NVarChar);
                    break;
                case "event_type":
                    builder.AppendParameter((object?)Truncate(p.EventTypeName, 500), SqlDbType.NVarChar);
                    break;
                case "event_id":
                    builder.AppendParameter((object?)p.EventId, SqlDbType.UniqueIdentifier);
                    break;
                case "failing_stream":
                    builder.AppendParameter((object?)Truncate(p.FailingStream, 500), SqlDbType.NVarChar);
                    break;
                case "parked_at":
                    if (p.ParkedAt == default)
                    {
                        builder.Append("SYSDATETIMEOFFSET()");
                    }
                    else
                    {
                        builder.AppendParameter(p.ParkedAt, SqlDbType.DateTimeOffset);
                    }

                    break;
                case "replay_attempts":
                    builder.AppendParameter(p.ReplayAttempts, SqlDbType.Int);
                    break;
                case "last_attempt_at":
                    builder.AppendParameter((object?)p.LastAttemptAt, SqlDbType.DateTimeOffset);
                    break;
                case "last_attempt_error":
                    builder.AppendParameter((object?)p.LastAttemptError, SqlDbType.NVarChar);
                    break;
                case "requested_action":
                    builder.AppendParameter((object?)p.RequestedAction?.ToString(), SqlDbType.VarChar);
                    break;
                case "requested_at":
                    builder.AppendParameter((object?)p.RequestedAt, SqlDbType.DateTimeOffset);
                    break;
            }
        }
    }

    internal static SqlDbType KeyColumns(bool unicode) => Polecat.Storage.KeyColumns.DbType(unicode);

    internal static string? Truncate(string? value, int length)
        => value == null || value.Length <= length ? value : value[..length];

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}

/// <summary>
///     marten#6003 / #1053: delete the parked row of an identity in a daemon batch, so the unpark commits with the
///     re-folded document and the batch's progression. Deleting a row that isn't there is not an error.
/// </summary>
internal sealed class UnparkIdentityOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly string _table;
    private readonly ParkedIdentityKey _key;
    private readonly bool _unicode;

    public UnparkIdentityOperation(string table, ParkedIdentityKey key, bool unicode)
    {
        _table = table;
        _key = key;
        _unicode = unicode;
    }

    public Type DocumentType => typeof(object);

    public OperationRole Role() => OperationRole.Deletion;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var key = ParkIdentityOperation.KeyColumns(_unicode);

        builder.Append($"DELETE FROM {_table} WHERE projection_name = ");
        builder.AppendParameter(_key.ProjectionName, key);
        builder.Append(" AND projection_version = ");
        builder.AppendParameter((int)_key.ProjectionVersion, SqlDbType.Int);
        builder.Append(" AND shard_key = ");
        builder.AppendParameter(_key.ShardKey, key);
        builder.Append(" AND tenant_id = ");
        builder.AppendParameter(_key.TenantId, key);
        builder.Append(" AND identity_text = ");
        builder.AppendParameter(_key.IdentityText, key);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}

/// <summary>
///     marten#6003 / #1053: write slice index rows in a daemon batch, so they commit with the documents and the
///     progression of the events they describe. Insert-if-absent: a page read again after a failed batch, or the
///     slices of a rebuild, write rows that may already be there. One statement however many rows, through one JSON
///     parameter, so a large page can't run into SQL Server's 2,100 parameter limit.
/// </summary>
internal sealed class AppendSliceIndexOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly string _table;
    private readonly SliceIndexRows _rows;
    private readonly bool _unicode;

    public AppendSliceIndexOperation(string table, SliceIndexRows rows, bool unicode)
    {
        _table = table;
        _rows = rows;
        _unicode = unicode;
    }

    public Type DocumentType => typeof(object);

    public OperationRole Role() => OperationRole.Insert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var simple = SliceIndexTable.SimpleIdentityType(_rows.IdentityType);
        var identityType = EventTagTable.SqlServerTypeFor(simple, _unicode);
        var tenantType = Polecat.Storage.KeyColumns.Text(_unicode);

        builder.Append($"INSERT INTO {_table} (tenant_id, identity_value, seq_id) SELECT DISTINCT j.t, j.i, j.s FROM OPENJSON(");
        builder.AppendParameter(ToJson(_rows.Entries), SqlDbType.NVarChar);
        builder.Append($") WITH (t {tenantType} '$.t', i {identityType} '$.i', s bigint '$.s') j ");
        builder.Append($"WHERE NOT EXISTS (SELECT 1 FROM {_table} x WITH (UPDLOCK, HOLDLOCK) WHERE x.tenant_id = j.t AND x.identity_value = j.i AND x.seq_id = j.s);");
    }

    internal static string ToJson(IReadOnlyList<SliceIndexEntry> entries)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var entry in entries)
            {
                writer.WriteStartObject();
                writer.WriteString("t", entry.TenantId);
                switch (ParkedIdentityText.Unwrap(entry.Identity))
                {
                    case Guid guid:
                        writer.WriteString("i", guid.ToString("D"));
                        break;
                    case string text:
                        writer.WriteString("i", text);
                        break;
                    case int number:
                        writer.WriteNumber("i", number);
                        break;
                    case long number:
                        writer.WriteNumber("i", number);
                        break;
                }

                writer.WriteNumber("s", entry.Sequence);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
