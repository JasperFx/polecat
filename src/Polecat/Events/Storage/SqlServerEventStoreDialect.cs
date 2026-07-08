using System;
using System.Data;
using JasperFx.Events;
using Polecat.Exceptions;
using Polecat.Storage;
using Weasel.Core;
using Weasel.Storage;

namespace Polecat.Events.Storage;

/// <summary>
///     SQL Server implementation of the closed-shape event-storage dialect seam
///     (<see cref="IEventStoreSqlDialect" />, Weasel.Storage). The SQL-Server counterpart of
///     Marten's <c>PostgresEventStoreDialect</c> — it builds the per-append-mode descriptors that
///     drive the shared <see cref="EventStorage{TId}" /> hierarchy. Polecat is QuickAppend-only
///     (direct INSERTs, no stored procedures — see CLAUDE.md), so only the Quick descriptor is
///     implemented; Rich and QuickWithServerTimestamps are not reachable and throw.
/// </summary>
/// <remarks>
///     <para>
///         Unlike the Postgres dialect, Polecat has no <c>mt_quick_append_events</c> server
///         function: the batched append is a direct multi-statement <c>INSERT ... OUTPUT
///         inserted.seq_id</c> supplied through <see cref="QuickEventStorageDescriptor.CreateQuickAppendEventsOperation" />
///         (<see cref="PolecatQuickAppendEventsOperation" />). Stream-row management uses the shared
///         <c>QuickInsertStreamOperation</c> / <c>QuickUpdateStreamVersionOperation</c> /
///         <c>AssertStreamVersionOperation</c> via the descriptor's command-configurer closures.
///     </para>
///     <para>
///         Wired behind the opt-in <see cref="EventStoreOptions.UseClosedShapeEventStorage" /> flag
///         (#273 event-dialect increment 1); the bespoke inline append path in
///         <c>DocumentSessionBase</c> remains the default.
///     </para>
/// </remarks>
internal sealed class SqlServerEventStoreDialect : IEventStoreSqlDialect
{
    public RichEventStorageDescriptor BuildRichDescriptor(EventRegistry graph, IStorageSerializer serializer)
        => throw new NotSupportedException(
            "Polecat is QuickAppend-only; the Rich (full-mode) event append path is not supported.");

    public QuickWithServerTimestampsEventStorageDescriptor BuildQuickWithServerTimestampsDescriptor(
        EventRegistry graph, IStorageSerializer serializer)
        => throw new NotSupportedException(
            "Polecat is QuickAppend-only; the QuickWithServerTimestamps event append path is not supported.");

    public QuickEventStorageDescriptor BuildQuickDescriptor(EventRegistry registry, IStorageSerializer serializer)
    {
        var graph = (EventGraph)registry;
        var isGuid = graph.StreamIdentity == StreamIdentity.AsGuid;
        var isConjoined = graph.TenancyStyle == TenancyStyle.Conjoined;
        var dialect = ResolveStorageDialect(isGuid);
        var options = graph.EventOptions;

        return new QuickEventStorageDescriptor(
            quickAppendEventsSql: $"insert into {graph.EventsTableName} ",
            insertStreamSql: $"insert into {graph.StreamsTableName} (...) values (...)",
            updateStreamVersionSql: $"update {graph.StreamsTableName} set version = ... where ...",
            // Polecat is STJ/JSON-only — no binary event serialization. Data is always JSON; bdata is
            // always null. Serialize through the session serializer at write time (see the append op);
            // the descriptor closure below covers any shared-op path that reaches for it.
            serializeEventData: e => serializer.ToJson(e.Data),
            serializeEventBdata: _ => null)
        {
            IsGuidStreamIdentity = isGuid,
            Dialect = dialect,
            IsTenancyConjoined = isConjoined,
            AssertStreamVersionSql = $"select version from {graph.StreamsTableName} where id = ",
            HasCausationId = options.EnableCausationId,
            HasCorrelationId = options.EnableCorrelationId,
            HasHeaders = options.EnableHeaders,
            HasUserName = options.EnableUserName,
            ConfigureInsertStreamCommand = BuildInsertStreamCommandConfigurer(graph, isGuid, dialect),
            TransformInsertStreamException = MapInsertStreamException,
            ConfigureUpdateStreamVersionCommand = BuildUpdateStreamVersionCommandConfigurer(graph, isGuid, isConjoined, dialect),
            // SQL Server has no array-parameter bulk function; the batched append is a direct
            // multi-row INSERT ... OUTPUT inserted.seq_id owned by the dialect.
            CreateQuickAppendEventsOperation = (descriptor, stream) =>
                new PolecatQuickAppendEventsOperation(graph, descriptor, stream)
        };
    }

    /// <summary>
    ///     The SQL Server <see cref="IStorageDialect" /> the descriptor threads to the shared ops for
    ///     provider parameter typing. Reuses the doc-side dialect (#273 phase D); the stream-identity
    ///     generic only affects id typing, which the ops set explicitly anyway.
    /// </summary>
    private static IStorageDialect ResolveStorageDialect(bool isGuid)
        => isGuid ? SqlServerStorageDialect<Guid>.Instance : SqlServerStorageDialect<string>.Instance;

    /// <summary>
    ///     Closure for <c>insert into {schema}.pc_streams (id, type, version, timestamp, created,
    ///     tenant_id) values (@id, @type, @version, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(),
    ///     @tenant_id)</c>. Mirrors the bespoke <c>ProcessStartStreamAsync</c> insert; columns are
    ///     named so the same statement serves both single-tenant and conjoined tables.
    /// </summary>
    private static Action<Weasel.Core.ICommandBuilder, StreamAction> BuildInsertStreamCommandConfigurer(
        EventGraph graph, bool isGuid, IStorageDialect dialect)
    {
        var prefix = $"insert into {graph.StreamsTableName} " +
                     "(id, type, version, timestamp, created, tenant_id) values (";

        return (builder, stream) =>
        {
            builder.Append(prefix);

            var idParam = builder.AppendParameter(isGuid ? stream.Id : (object)stream.Key!);
            dialect.SetParameterType(idParam, isGuid ? StorageColumnType.Guid : StorageColumnType.String);

            builder.Append(", ");
            var typeParam = builder.AppendParameter((object?)stream.AggregateType?.Name ?? DBNull.Value);
            dialect.SetParameterType(typeParam, StorageColumnType.String);

            builder.Append(", ");
            var versionParam = builder.AppendParameter(stream.Version);
            dialect.SetParameterType(versionParam, StorageColumnType.Long);

            builder.Append(", SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), ");
            var tenantParam = builder.AppendParameter(stream.TenantId);
            dialect.SetParameterType(tenantParam, StorageColumnType.String);

            builder.Append(")");
        };
    }

    /// <summary>
    ///     Closure for <c>update {schema}.pc_streams set version = @version, timestamp =
    ///     SYSDATETIMEOFFSET() where id = @id and version = @expected [and tenant_id = @tenant]</c>.
    ///     The <c>and version = @expected</c> guard is the shared expected-version check; Polecat's
    ///     planner reads the current version under <c>UPDLOCK, HOLDLOCK</c> first, so the guard always
    ///     matches, but it stays as defense-in-depth and to satisfy the shared op's Postprocess
    ///     (0-rows → <c>EventStreamUnexpectedMaxEventIdException</c>).
    /// </summary>
    private static Action<Weasel.Core.ICommandBuilder, StreamAction> BuildUpdateStreamVersionCommandConfigurer(
        EventGraph graph, bool isGuid, bool isConjoined, IStorageDialect dialect)
    {
        var prefix = $"update {graph.StreamsTableName} set version = ";

        return (builder, stream) =>
        {
            builder.Append(prefix);
            var versionParam = builder.AppendParameter(stream.Version);
            dialect.SetParameterType(versionParam, StorageColumnType.Long);

            builder.Append(", timestamp = SYSDATETIMEOFFSET() where id = ");
            var idParam = builder.AppendParameter(isGuid ? stream.Id : (object)stream.Key!);
            dialect.SetParameterType(idParam, isGuid ? StorageColumnType.Guid : StorageColumnType.String);

            builder.Append(" and version = ");
            var expectedParam = builder.AppendParameter(stream.ExpectedVersionOnServer!.Value);
            dialect.SetParameterType(expectedParam, StorageColumnType.Long);

            if (isConjoined)
            {
                builder.Append(" and tenant_id = ");
                var tenantParam = builder.AppendParameter(stream.TenantId);
                dialect.SetParameterType(tenantParam, StorageColumnType.String);
            }
        };
    }

    /// <summary>
    ///     Maps a SQL Server primary-key violation (error 2627) on the pc_streams insert to
    ///     <see cref="Exceptions.ExistingStreamIdCollisionException" />. Mirrors the bespoke path's
    ///     catch in <c>ProcessStartStreamAsync</c>. Returns null for any other exception.
    /// </summary>
    private static Exception? MapInsertStreamException(Exception original, StreamAction stream)
    {
        var sql = original as Microsoft.Data.SqlClient.SqlException
                  ?? original.InnerException as Microsoft.Data.SqlClient.SqlException;
        if (sql is { Number: 2627 })
        {
            var id = stream.Key is not null ? (object)stream.Key : stream.Id;
            return new ExistingStreamIdCollisionException(id);
        }

        return null;
    }
}
