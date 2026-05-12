using System.Data.Common;
using JasperFx.Descriptors;
using JasperFx.Events;
using Polecat.Metadata;

namespace Polecat.Events.Internal;

/// <summary>
///     Canonical SQL fragment + row reader for the <c>pc_streams</c> table.
///     Shared between <see cref="QueryEventStore.FetchStreamStateAsync(System.Guid, System.Threading.CancellationToken)"/>
///     and the <see cref="JasperFx.Events.IEventStore"/> explorer surface so the
///     column shape stays in sync across schema migrations. Closes the audit
///     row tracked at
///     <see href="https://github.com/JasperFx/polecat/issues/57">polecat#57</see>
///     (mirrors the Marten 9 <c>ISelector&lt;StreamState&gt;</c> consolidation).
/// </summary>
/// <remarks>
///     The canonical column order is locked here. Call sites compose
///     <c>SELECT {<see cref="SelectColumns"/>} FROM {streamsTable} ...</c> and
///     pass the resulting reader into one of the typed read methods rather
///     than hand-rolling positional <c>GetInt64</c> / <c>GetGuid</c> ladders.
///     Adding or renaming a <c>pc_streams</c> column means updating this file
///     and exactly this file.
/// </remarks>
internal static class PcStreamsRowReader
{
    /// <summary>
    ///     Canonical column projection for any read from <c>pc_streams</c>. The
    ///     order is fixed so the typed read methods below can use positional
    ///     reads. When this changes, every call site that composes its SELECT
    ///     from this constant adjusts automatically.
    /// </summary>
    internal const string SelectColumns = "id, type, version, created, timestamp, tenant_id, is_archived";

    /// <summary>
    ///     Same column list with a table alias prefix, for queries that join
    ///     <c>pc_streams</c> against other tables.
    /// </summary>
    internal static string SelectColumnsWithAlias(string alias) =>
        $"{alias}.id, {alias}.type, {alias}.version, {alias}.created, {alias}.timestamp, {alias}.tenant_id, {alias}.is_archived";

    /// <summary>
    ///     Read the row the reader is positioned on into a
    ///     <see cref="StreamState"/>. The caller is responsible for
    ///     <c>await reader.ReadAsync(...)</c> beforehand.
    /// </summary>
    internal static StreamState ReadStreamState(DbDataReader reader, StreamIdentity streamIdentity)
    {
        var state = new StreamState
        {
            Version = reader.GetInt64(2),
            Created = reader.GetFieldValue<DateTimeOffset>(3),
            LastTimestamp = reader.GetFieldValue<DateTimeOffset>(4),
            IsArchived = reader.GetBoolean(6)
        };

        if (streamIdentity == StreamIdentity.AsGuid)
        {
            state.Id = reader.GetGuid(0);
        }
        else
        {
            state.Key = reader.GetString(0);
        }

        return state;
    }

    /// <summary>
    ///     Read the row the reader is positioned on into a
    ///     <see cref="StreamSummary"/>. <paramref name="defaultTenantId"/> is
    ///     substituted when the row's <c>tenant_id</c> column is NULL (matches
    ///     Polecat's single-tenant fall-back behavior).
    /// </summary>
    internal static StreamSummary ReadStreamSummary(DbDataReader reader, string defaultTenantId)
    {
        var streamId = StreamIdToString(reader.GetValue(0));
        var streamType = reader.IsDBNull(1) ? null : reader.GetString(1);
        var version = reader.GetInt64(2);
        var created = reader.GetFieldValue<DateTimeOffset>(3);
        var lastUpdated = reader.GetFieldValue<DateTimeOffset>(4);
        var tenantId = reader.IsDBNull(5) ? defaultTenantId : reader.GetString(5);

        return new StreamSummary(streamId, streamType!, version, created, lastUpdated, tenantId);
    }

    /// <summary>
    ///     Read the row the reader is positioned on into a
    ///     <see cref="StreamMetadata"/>. <paramref name="firstEventAt"/> comes
    ///     from a JOIN against <c>pc_events</c> (not part of <c>pc_streams</c>
    ///     itself); when it's <see langword="null"/> the row's own <c>created</c>
    ///     column is used as the fallback, mirroring the pre-consolidation
    ///     behavior in <c>DocumentStore.GetStreamMetadataAsync</c>.
    /// </summary>
    internal static StreamMetadata ReadStreamMetadata(
        DbDataReader reader,
        string defaultTenantId,
        DateTimeOffset? firstEventAt)
    {
        var streamId = StreamIdToString(reader.GetValue(0));
        var streamType = reader.IsDBNull(1) ? null : reader.GetString(1);
        var version = reader.GetInt64(2);
        var created = reader.GetFieldValue<DateTimeOffset>(3);
        var lastUpdated = reader.GetFieldValue<DateTimeOffset>(4);
        var tenantId = reader.IsDBNull(5) ? defaultTenantId : reader.GetString(5);
        var isArchived = reader.GetBoolean(6);

        return new StreamMetadata(
            streamId,
            streamType!,
            version,
            firstEventAt ?? created,
            lastUpdated,
            LastSnapshotAt: null,
            LastSnapshotVersion: null,
            IsArchived: isArchived,
            TenantId: tenantId,
            Tags: null!);
    }

    private static string StreamIdToString(object value) => value switch
    {
        System.Guid g => g.ToString(),
        _ => value.ToString() ?? string.Empty
    };
}
