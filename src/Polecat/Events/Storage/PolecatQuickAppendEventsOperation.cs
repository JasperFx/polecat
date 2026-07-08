using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Exceptions;
using JasperFx.Events;
using Polecat.Exceptions;
using Weasel.Core;
using Weasel.Storage;

namespace Polecat.Events.Storage;

/// <summary>
///     Whether the closed-shape append writes the stream row with an INSERT (new stream / start) or
///     an UPDATE (existing stream). Decided by the append planner after its version read.
/// </summary>
internal enum StreamWriteMode
{
    Insert,
    Update
}

/// <summary>
///     SQL Server batched append operation for the closed-shape Quick event storage (#273
///     event-dialect increment 1) — the dialect-supplied counterpart of Marten's Postgres
///     <c>QuickAppendEventsOperation</c>. SQL Server has no array-parameter server function, so this
///     emits one self-contained command per stream: the stream-row write (via the descriptor's
///     insert/update-version closure), one <c>INSERT INTO pc_events ... OUTPUT inserted.seq_id INTO
///     @table</c> per event, and a single trailing <c>SELECT</c> of the assigned <c>seq_id</c>s.
/// </summary>
/// <remarks>
///     <para>
///         Exactly one client result set per operation (the trailing SELECT), matching the shared
///         batch executor's one-result-set-per-operation contract; <c>OUTPUT ... INTO</c> keeps the
///         per-event inserts from returning their own result sets. Event <see cref="IEvent.Version" />s
///         are pre-assigned by the planner (which reads the current stream version under
///         <c>UPDLOCK, HOLDLOCK</c>), so this operation only writes and reads sequences back onto
///         <see cref="IEvent.Sequence" /> in <see cref="PostprocessAsync" />.
///     </para>
///     <para>
///         Increment 1 covers the core append shape (single/conjoined tenancy, Guid/string streams,
///         optional correlation/causation/headers/user_name columns). DCB tag writes and per-tenant
///         event partitioning are not yet supported and throw; the bespoke inline path still covers
///         them.
///     </para>
/// </remarks>
internal sealed class PolecatQuickAppendEventsOperation
    : Weasel.Storage.IStorageOperation, IExceptionTransform
{
    private const string SeqTableVar = "@pc_appended_seqs";

    private readonly EventGraph _graph;
    private readonly QuickEventStorageDescriptor _descriptor;

    public PolecatQuickAppendEventsOperation(EventGraph graph, QuickEventStorageDescriptor descriptor,
        StreamAction stream)
    {
        _graph = graph;
        _descriptor = descriptor;
        Stream = stream;
    }

    public StreamAction Stream { get; }

    /// <summary>Set by the planner: INSERT a new stream row, or UPDATE the existing one's version.</summary>
    public StreamWriteMode Mode { get; set; } = StreamWriteMode.Insert;

    public Type DocumentType => typeof(IEvent);

    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(Weasel.Core.ICommandBuilder builder, IStorageSession session)
    {
        if (_graph.UseTenantPartitionedEvents)
            throw new NotSupportedException(
                "Per-tenant event partitioning is not yet supported on the closed-shape event append path.");

        builder.Append("declare ");
        builder.Append(SeqTableVar);
        builder.Append(" table (ord int identity(1,1), seq_id bigint);");

        // Stream row write reuses the dialect's descriptor closures so the SQL stays in one place.
        if (Mode == StreamWriteMode.Insert)
            _descriptor.ConfigureInsertStreamCommand(builder, Stream);
        else
            _descriptor.ConfigureUpdateStreamVersionCommand(builder, Stream);
        builder.Append(";");

        var options = _graph.EventOptions;

        foreach (var @event in Stream.Events)
        {
            if (@event.Tags is { Count: > 0 } && _graph.TagTypes.Count > 0)
                throw new NotSupportedException(
                    "DCB tag writes are not yet supported on the closed-shape event append path.");

            builder.Append("insert into ");
            builder.Append(_graph.EventsTableName);
            builder.Append(" (id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type");
            if (options.EnableCorrelationId) builder.Append(", correlation_id");
            if (options.EnableCausationId) builder.Append(", causation_id");
            if (options.EnableHeaders) builder.Append(", headers");
            if (options.EnableUserName) builder.Append(", user_name");
            builder.Append(") output inserted.seq_id into ");
            builder.Append(SeqTableVar);
            builder.Append(" values (");

            Bind(builder, @event.Id, StorageColumnType.Guid);

            builder.Append(", ");
            if (_descriptor.IsGuidStreamIdentity)
                Bind(builder, Stream.Id, StorageColumnType.Guid);
            else
                Bind(builder, Stream.Key!, StorageColumnType.String);

            builder.Append(", ");
            Bind(builder, @event.Version, StorageColumnType.Long);

            builder.Append(", ");
            Bind(builder, session.Serializer.ToJson(@event.Data), StorageColumnType.Json);

            builder.Append(", ");
            Bind(builder, @event.EventTypeName, StorageColumnType.String);

            builder.Append(", SYSDATETIMEOFFSET(), ");
            Bind(builder, @event.TenantId ?? Stream.TenantId, StorageColumnType.String);

            builder.Append(", ");
            Bind(builder, @event.DotNetTypeName, StorageColumnType.String);

            if (options.EnableCorrelationId)
            {
                builder.Append(", ");
                Bind(builder, (object?)@event.CorrelationId ?? DBNull.Value, StorageColumnType.String);
            }

            if (options.EnableCausationId)
            {
                builder.Append(", ");
                Bind(builder, (object?)@event.CausationId ?? DBNull.Value, StorageColumnType.String);
            }

            if (options.EnableHeaders)
            {
                builder.Append(", ");
                object headers = @event.Headers is { Count: > 0 }
                    ? session.Serializer.ToJson(@event.Headers)
                    : DBNull.Value;
                Bind(builder, headers, StorageColumnType.Json);
            }

            if (options.EnableUserName)
            {
                builder.Append(", ");
                Bind(builder, (object?)@event.UserName ?? DBNull.Value, StorageColumnType.String);
            }

            builder.Append(");");
        }

        builder.Append("select seq_id from ");
        builder.Append(SeqTableVar);
        builder.Append(" order by ord;");
    }

    private void Bind(Weasel.Core.ICommandBuilder builder, object value, StorageColumnType type)
    {
        var parameter = builder.AppendParameter(value);
        _descriptor.Dialect.SetParameterType(parameter, type);
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        // Single result set (the trailing SELECT), one row per event in append order.
        var events = Stream.Events;
        var i = 0;
        while (i < events.Count && await reader.ReadAsync(token).ConfigureAwait(false))
        {
            events[i].Sequence = await reader.GetFieldValueAsync<long>(0, token).ConfigureAwait(false);
            i++;
        }
    }

    /// <summary>
    ///     Map a SQL Server primary-key violation (2627) on the stream INSERT to
    ///     <see cref="ExistingStreamIdCollisionException" /> — the closed-shape equivalent of the
    ///     bespoke <c>ProcessStartStreamAsync</c> catch. Only meaningful when this op inserts the
    ///     stream row.
    /// </summary>
    public bool TryTransform(Exception original, out Exception? transformed)
    {
        if (Mode == StreamWriteMode.Insert)
        {
            var sql = original as Microsoft.Data.SqlClient.SqlException
                      ?? original.InnerException as Microsoft.Data.SqlClient.SqlException;
            if (sql is { Number: 2627 })
            {
                var id = Stream.Key is not null ? (object)Stream.Key : Stream.Id;
                transformed = new ExistingStreamIdCollisionException(id);
                return true;
            }
        }

        transformed = null;
        return false;
    }
}
