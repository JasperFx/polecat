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
///     @table</c> per event, and a single trailing <c>SELECT</c> of the assigned <c>seq_id</c>s. A
///     stream too big for one command is written by several of these; see
///     <see cref="SplitToFit" />.
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

    // #834: the run of Stream.Events this operation writes. The whole stream, unless SplitToFit cut it.
    private int _firstEvent;
    private int _eventCount;

    public PolecatQuickAppendEventsOperation(EventGraph graph, QuickEventStorageDescriptor descriptor,
        StreamAction stream)
    {
        _graph = graph;
        _descriptor = descriptor;
        Stream = stream;
        _eventCount = stream.Events.Count;
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

    /// <summary>
    ///     #834: this append as however many operations it takes to keep each one's command within
    ///     <paramref name="budget" /> parameters. An append that already fits comes back as itself, so
    ///     an ordinary save has exactly the one command it always had. Call it after the planner has
    ///     set <see cref="Mode" /> and the partition properties, because every slice carries them.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>The limit belongs to a command, not to a batch.</b> Measured on SQL Server 2025:
    ///         one <c>SqlBatch</c> carrying 100 commands of 2000 parameters each runs, while one
    ///         command of 2099 is refused with error 8003. Each command is an <c>sp_executesql</c>
    ///         call, whose statement and parameter declaration take two of the 2100. So a stream only
    ///         has to become more commands, and they all travel in the batch this append was already
    ///         in: same round trip, same transaction, however large the save.
    ///     </para>
    ///     <para>
    ///         <b>A slice is a whole operation rather than a second command inside this one</b>,
    ///         because two things depend on there being exactly one command per operation.
    ///         <see cref="Polecat.Internal.Operations.OperationResultSets" /> gives each operation one
    ///         result set, and each slice has one, the trailing <c>SELECT</c> of its own
    ///         <c>@pc_appended_seqs</c>, which is scoped to its command. And
    ///         <see cref="Polecat.Internal.BatchExceptionTransforms" /> finds the operation behind a
    ///         SQL error by matching the failing command's index, which is what lets a collision in a
    ///         later stream name that stream rather than this one.
    ///     </para>
    ///     <para>
    ///         Only the first slice writes the stream row, and it writes the stream's final version,
    ///         which the planner has already worked out. The later slices run inside the same
    ///         transaction, so the row's lock and its version are both already in place. Every slice
    ///         keeps <see cref="Mode" />, so a 2627 from any of them maps as it did from the one
    ///         command they replace.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>A slice always takes at least one event</b>, so an event that needs more than the
    ///         budget by itself still goes out whole and SQL Server refuses it. That is past 1043 tags
    ///         on one event, or 521 under conjoined tenancy, and it cannot be cut here: its tag rows
    ///         read the <c>seq_id</c> from a table variable that only exists in its own command.
    ///     </para>
    /// </remarks>
    internal IReadOnlyList<PolecatQuickAppendEventsOperation> SplitToFit(int budget)
    {
        var perEvent = ParametersPerEvent();
        var events = Stream.Events;

        List<PolecatQuickAppendEventsOperation>? slices = null;
        var first = 0;
        var bound = ParametersForStreamRow();

        for (var i = 0; i < events.Count; i++)
        {
            var cost = perEvent + ParametersForTags(events[i]);

            if (i > first && bound + cost > budget)
            {
                (slices ??= new List<PolecatQuickAppendEventsOperation>()).Add(Slice(first, i - first));
                first = i;
                bound = 0;
            }

            bound += cost;
        }

        if (slices is null) return [this];

        slices.Add(Slice(first, events.Count - first));
        return slices;
    }

    private PolecatQuickAppendEventsOperation Slice(int firstEvent, int eventCount)
        => new(_graph, _descriptor, Stream)
        {
            Mode = Mode,
            PartitionOrdinal = PartitionOrdinal,
            PartitionSequenceName = PartitionSequenceName,
            _firstEvent = firstEvent,
            _eventCount = eventCount
        };

    private bool WritesStreamRow => _firstEvent == 0;

    // These three restate what ConfigureCommand binds. appending_past_the_parameter_limit_tests holds
    // them against the parameters a real builder ends up with, for every column toggle, so a column
    // added below without a change here fails there rather than at 2100 in someone's production save.
    private int ParametersPerEvent()
    {
        var options = _graph.EventOptions;

        // id, stream_id, version, data, bdata, type, tenant_id, dotnet_type
        var count = 8;
        if (_graph.UseTenantPartitionedEvents) count++;
        if (options.EnableCorrelationId) count++;
        if (options.EnableCausationId) count++;
        if (options.EnableHeaders) count++;
        if (options.EnableUserName) count++;

        return count;
    }

    private int ParametersForStreamRow()
    {
        var partitioned = _graph.UseTenantPartitionedEvents ? 1 : 0;
        if (Mode == StreamWriteMode.Insert)
        {
            // id, type, version, tenant_id
            return 4 + partitioned;
        }

        // version, id, expected version, and the tenant when conjoined
        var conjoined = _graph.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined ? 1 : 0;
        return 3 + conjoined + partitioned;
    }

    private int ParametersForTags(IEvent @event)
    {
        if (_graph.TagTypes.Count == 0 || @event.Tags is not { Count: > 0 } tags) return 0;

        // WriteTagInserts binds the value (and the tenant, when conjoined) in both the guard and the insert.
        var perTag = _graph.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined ? 4 : 2;
        return perTag * tags.Count;
    }

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
        var events = Stream.Events;
        var lastEvent = _firstEvent + _eventCount;

        var writesTags = false;
        if (_graph.TagTypes.Count > 0)
        {
            for (var i = _firstEvent; i < lastEvent && !writesTags; i++)
            {
                writesTags = events[i].Tags is { Count: > 0 };
            }
        }

        if (writesTags)
        {
            builder.Append("declare ");
            builder.Append(SeqIdVar);
            builder.Append(" bigint;");
        }

        // Stream row write reuses the dialect's descriptor closures so the SQL stays in one place.
        if (WritesStreamRow)
        {
            if (Mode == StreamWriteMode.Insert)
                _descriptor.ConfigureInsertStreamCommand(builder, Stream);
            else
                _descriptor.ConfigureUpdateStreamVersionCommand(builder, Stream);
            builder.Append(";");
        }

        var options = _graph.EventOptions;
        var conjoined = _graph.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined;
        var archivePartitioned = _graph.UseArchivedStreamPartitioning;

        // The ordinal is the event's position in THIS command, because that is what the identity
        // column of this command's own @pc_appended_seqs numbers.
        var ordinal = 0;
        for (var eventIndex = _firstEvent; eventIndex < lastEvent; eventIndex++)
        {
            var @event = events[eventIndex];
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

            builder.Append("id, stream_id, version, data, bdata, type, timestamp, tenant_id, dotnet_type");
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

            // #388: a binary event writes the '{}' placeholder to `data` and its payload to `bdata`;
            // a JSON event writes the payload to `data` and NULL to `bdata`. Exactly one of the two
            // is populated per row, and `bdata IS NULL` is what the read path dispatches on.
            var bdata = _graph.SerializeEventBdata(@event);

            builder.Append(", ");
            Bind(builder,
                bdata is null ? session.Serializer.ToJson(@event.Data) : EventGraph.JsonPlaceholderForBinaryEvent,
                StorageColumnType.Json);

            builder.Append(", ");
            Bind(builder, (object?)bdata ?? DBNull.Value, StorageColumnType.Binary);

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
            var table = _graph.TagTableName(registration);

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
        // Single result set (the trailing SELECT), one row per event of this operation in append order.
        var events = Stream.Events;
        var i = _firstEvent;
        var lastEvent = _firstEvent + _eventCount;
        while (i < lastEvent && await reader.ReadAsync(token).ConfigureAwait(false))
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
                transformed = new Polecat.Exceptions.ExistingStreamIdCollisionException(id);
                return true;
            }
        }

        transformed = null;
        return false;
    }
}
