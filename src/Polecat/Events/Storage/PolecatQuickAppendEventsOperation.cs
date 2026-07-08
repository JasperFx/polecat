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
///         Covers the core append shape (single/conjoined tenancy, Guid/string streams, optional
///         correlation/causation/headers/user_name columns), DCB tag writes, and per-tenant event
///         partitioning (UseTenantPartitionedEvents — seq_id via NEXT VALUE FOR the tenant sequence
///         plus the tenant_ordinal column, resolved by the planner).
///     </para>
/// </remarks>
internal sealed class PolecatQuickAppendEventsOperation
    : Weasel.Storage.IStorageOperation, IExceptionTransform
{
    private const string SeqTableVar = "@pc_appended_seqs";
    private const string SeqIdVar = "@pc_event_seq";

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

    /// <summary>
    ///     Per-tenant partitioning (<c>UseTenantPartitionedEvents</c>): the stream tenant's compact
    ///     partition ordinal, resolved (and provisioned) by the planner. Null when partitioning is off,
    ///     in which case <c>seq_id</c> is the global IDENTITY column.
    /// </summary>
    public int? PartitionOrdinal { get; set; }

    /// <summary>
    ///     The stream tenant's per-tenant sequence name (schema-qualified) that feeds <c>seq_id</c> via
    ///     <c>NEXT VALUE FOR</c> under per-tenant partitioning. Set alongside <see cref="PartitionOrdinal" />.
    /// </summary>
    public string? PartitionSequenceName { get; set; }

    public Type DocumentType => typeof(IEvent);

    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(Weasel.Core.ICommandBuilder builder, IStorageSession session)
    {
        var partitioned = _graph.UseTenantPartitionedEvents;
        if (partitioned && (PartitionSequenceName is null || PartitionOrdinal is null))
            throw new InvalidOperationException(
                "Per-tenant partitioning is enabled but the planner did not resolve the tenant partition " +
                "ordinal/sequence for this append operation.");

        builder.Append("declare ");
        builder.Append(SeqTableVar);
        builder.Append(" table (ord int identity(1,1), seq_id bigint);");

        // DCB tag writes need the just-inserted event's seq_id; captured per event via
        // SCOPE_IDENTITY() (seq_id is IDENTITY on this path — per-tenant partitioning, which uses a
        // sequence instead, is rejected above).
        var writesTags = _graph.TagTypes.Count > 0
            && Stream.Events.Any(e => e.Tags is { Count: > 0 });
        if (writesTags)
        {
            builder.Append("declare ");
            builder.Append(SeqIdVar);
            builder.Append(" bigint;");
        }

        // Stream row write reuses the dialect's descriptor closures so the SQL stays in one place.
        if (Mode == StreamWriteMode.Insert)
            _descriptor.ConfigureInsertStreamCommand(builder, Stream);
        else
            _descriptor.ConfigureUpdateStreamVersionCommand(builder, Stream);
        builder.Append(";");

        var options = _graph.EventOptions;
        var conjoined = _graph.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined;
        var archivePartitioned = _graph.UseArchivedStreamPartitioning;

        var ordinal = 0;
        foreach (var @event in Stream.Events)
        {
            ordinal++;
            builder.Append("insert into ");
            builder.Append(_graph.EventsTableName);
            builder.Append(" (");
            // Per-tenant partitioning: seq_id is drawn from the tenant's own sequence (not IDENTITY) and
            // the row carries the tenant's physical-partition ordinal.
            if (partitioned)
            {
                builder.Append("seq_id, ");
                builder.Append(_graph.TenantPartitionManager.Column);
                builder.Append(", ");
            }

            builder.Append("id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type");
            if (options.EnableCorrelationId) builder.Append(", correlation_id");
            if (options.EnableCausationId) builder.Append(", causation_id");
            if (options.EnableHeaders) builder.Append(", headers");
            if (options.EnableUserName) builder.Append(", user_name");
            builder.Append(") output inserted.seq_id into ");
            builder.Append(SeqTableVar);
            builder.Append(" values (");

            if (partitioned)
            {
                builder.Append("next value for ");
                builder.Append(PartitionSequenceName!);
                builder.Append(", ");
                Bind(builder, PartitionOrdinal!.Value, StorageColumnType.Int);
                builder.Append(", ");
            }

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

            if (writesTags && @event.Tags is { Count: > 0 })
            {
                WriteTagInserts(builder, @event, ordinal, conjoined, archivePartitioned);
            }
        }

        builder.Append("select seq_id from ");
        builder.Append(SeqTableVar);
        builder.Append(" order by ord;");
    }

    /// <summary>
    ///     Emits the per-event DCB tag-table upserts, mirroring the bespoke path's tenancy/archive
    ///     variants. Uses <c>SCOPE_IDENTITY()</c> for the event's just-assigned <c>seq_id</c>, and an
    ///     <c>IF NOT EXISTS</c> guard so a re-appended tag value is idempotent.
    /// </summary>
    private void WriteTagInserts(Weasel.Core.ICommandBuilder builder, IEvent @event, int ordinal,
        bool conjoined, bool archivePartitioned)
    {
        // Read the event's just-assigned seq_id back from the accumulator by its 1-based insert
        // ordinal. Deterministic and batch-safe — SCOPE_IDENTITY() is not reliably per-statement
        // when multiple stream operations share a SqlBatch scope.
        builder.Append("set ");
        builder.Append(SeqIdVar);
        builder.Append(" = (select seq_id from ");
        builder.Append(SeqTableVar);
        builder.Append(" where ord = ");
        builder.Append(ordinal.ToString(System.Globalization.CultureInfo.InvariantCulture));
        builder.Append(");");

        var tenantId = @event.TenantId ?? Stream.TenantId;

        foreach (var tag in @event.Tags!)
        {
            var registration = _graph.FindTagType(tag.TagType);
            if (registration == null) continue;

            var value = registration.ExtractValue(tag.Value);
            var table = $"[{_graph.DatabaseSchemaName}].[pc_event_tag_{registration.TableSuffix}]";

            builder.Append("if not exists (select 1 from ");
            builder.Append(table);
            builder.Append(" where value = ");
            BindTagValue(builder, value);
            if (conjoined)
            {
                builder.Append(" and tenant_id = ");
                Bind(builder, tenantId, StorageColumnType.String);
            }

            builder.Append(" and seq_id = ");
            builder.Append(SeqIdVar);
            if (archivePartitioned) builder.Append(" and is_archived = 0");

            builder.Append(") insert into ");
            builder.Append(table);
            builder.Append(conjoined
                ? (archivePartitioned
                    ? " (value, tenant_id, seq_id, is_archived) values ("
                    : " (value, tenant_id, seq_id) values (")
                : (archivePartitioned
                    ? " (value, seq_id, is_archived) values ("
                    : " (value, seq_id) values ("));

            BindTagValue(builder, value);
            if (conjoined)
            {
                builder.Append(", ");
                Bind(builder, tenantId, StorageColumnType.String);
            }

            builder.Append(", ");
            builder.Append(SeqIdVar);
            if (archivePartitioned) builder.Append(", 0");
            builder.Append(");");
        }
    }

    private void Bind(Weasel.Core.ICommandBuilder builder, object value, StorageColumnType type)
    {
        var parameter = builder.AppendParameter(value);
        _descriptor.Dialect.SetParameterType(parameter, type);
    }

    /// <summary>
    ///     Binds a DCB tag value with the SQL Server type matching its registered simple type — the
    ///     command builder pre-types fresh parameters as strings, so the type must be set explicitly
    ///     (Guid/int/long tags would otherwise bind as text).
    /// </summary>
    private void BindTagValue(Weasel.Core.ICommandBuilder builder, object value)
    {
        var (bindValue, type) = value switch
        {
            Guid => (value, StorageColumnType.Guid),
            int => (value, StorageColumnType.Int),
            long => (value, StorageColumnType.Long),
            short s => ((object)(int)s, StorageColumnType.Int),
            _ => (value, StorageColumnType.String)
        };

        Bind(builder, bindValue, type);
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
