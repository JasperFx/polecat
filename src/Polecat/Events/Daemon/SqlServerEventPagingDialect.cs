using System.Buffers;
using System.Data;
using System.Text;
using System.Text.Json;
using Polecat.Linq.SqlGeneration;
using Weasel.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     #989: how <c>pc_events</c> is paged by sequence for the shared <see cref="EventLoaderBase" />. The
///     base owns paging, skip accounting, the ceiling and cancellation; this owns the SQL Server SQL.
/// </summary>
/// <remarks>
///     <para>
///         The column list is <see cref="Internal.PcEventsRowReader.ComposeSelectColumns" />, which is what
///         <see cref="PolecatEventLoader" /> reads back, with <c>seq_id</c> at ordinal 0 where the base's
///         <c>ReadSequence</c> expects it.
///     </para>
///     <para>
///         <b>The type filter matches on <c>dotnet_type</c>, not on the <c>type</c> alias</b>, because
///         <c>dotnet_type</c> is the sole input to CLR type resolution on every Polecat read path. A row
///         admitted by alias but resolved by .NET name could disagree with the filter that admitted it.
///         Upcast sources are the exception: they get a second disjunct, <c>OR type IN (...)</c>, because an
///         upcast source may have no CLR type left to carry a <c>dotnet_type</c> (#561, jasperfx#752).
///     </para>
///     <para>
///         The <c>dotnet_type IS NULL</c> disjunct sends a row with no stored .NET type to hydration rather
///         than excluding it in silence, so it fails, or is skipped, under the shard's error policy. A
///         filtered shard used to behave that way, and an unfiltered one still does.
///     </para>
///     <para>
///         ⚠️ <b>A list of any length renders</b> (weasel#846). The base has no client-side filter to fall
///         back on, so a list the server refuses fails the shard. Up to <see cref="JsonValueList.Threshold" />
///         names, each is its own <c>varchar</c> parameter, which keeps the optimizer on real literals and
///         leaves an ordinary shard's SQL exactly as it was before #989. Above it, the whole list is one
///         JSON array expanded by <c>OPENJSON</c>, the same switch #710 made for LINQ. The <c>WITH</c>
///         clause types the values as <c>varchar(500)</c>, the columns' own type, because OPENJSON's default
///         <c>nvarchar(max)</c> would put <c>CONVERT_IMPLICIT</c> on the column and lose the seek (#363).
///         This replaced the old >2,000-type fallback, which dropped the predicate and filtered client-side.
///     </para>
/// </remarks>
internal sealed class SqlServerEventPagingDialect : IEventPagingDialect
{
    private readonly string _selectColumns;
    private readonly string _eventsTable;

    // The allow list is fixed for a loader's lifetime and the base hands the same instance back on every
    // page, so its rendering is computed once rather than re-serialized per page.
    private RenderedTypeFilter? _typeFilter;

    internal SqlServerEventPagingDialect(StoreOptions options, EventGraph events)
    {
        _selectColumns = Internal.PcEventsRowReader.ComposeSelectColumns(options.Events);
        _eventsTable = events.EventsTableName;
    }

    public EventPageCommand BuildPageCommand(EventPageQuery query)
    {
        var parameters = new List<EventPageParameter>
        {
            new("@batchSize", query.BatchSize, DbType.Int32)
        };

        var sql = new StringBuilder("SELECT TOP(@batchSize) ")
            .Append(_selectColumns)
            .Append(" FROM ").Append(_eventsTable);

        AppendWhere(sql, parameters, query);
        sql.Append(" ORDER BY seq_id;");

        return new EventPageCommand(sql.ToString(), parameters);
    }

    // TOP(1) ... ORDER BY seq_id, never MIN(seq_id), per the contract (marten#5277).
    public EventPageCommand BuildNextMatchingSequenceCommand(EventPageQuery query)
    {
        var parameters = new List<EventPageParameter>();
        var sql = new StringBuilder("SELECT TOP(1) seq_id FROM ").Append(_eventsTable);

        AppendWhere(sql, parameters, query);
        sql.Append(" ORDER BY seq_id;");

        return new EventPageCommand(sql.ToString(), parameters);
    }

    private void AppendWhere(StringBuilder sql, List<EventPageParameter> parameters, EventPageQuery query)
    {
        sql.Append(" WHERE seq_id > @floor AND seq_id <= @ceiling");
        parameters.Add(new EventPageParameter("@floor", query.Floor, DbType.Int64));
        parameters.Add(new EventPageParameter("@ceiling", query.Ceiling, DbType.Int64));

        if (!query.IncludeArchivedEvents)
        {
            sql.Append(" AND is_archived = 0");
        }

        // #163: a per-tenant rebuild or subscription shard reads only its own tenant's rows. Bound as
        // varchar (#363): tenant_id is varchar.
        if (query.TenantId is not null)
        {
            sql.Append(" AND tenant_id = @tenant");
            parameters.Add(new EventPageParameter("@tenant", query.TenantId, DbType.AnsiString));
        }

        if (query.EventTypes.IsEmpty)
        {
            return;
        }

        var filter = TypeFilterFor(query.EventTypes);
        sql.Append(filter.Sql);
        parameters.AddRange(filter.Parameters);
    }

    private RenderedTypeFilter TypeFilterFor(EventTypeAllowList allowList)
    {
        var cached = _typeFilter;
        if (cached is not null && ReferenceEquals(cached.AllowList, allowList))
        {
            return cached;
        }

        var parameters = new List<EventPageParameter>();
        var sql = new StringBuilder(" AND (dotnet_type IS NULL OR dotnet_type IN (")
            .Append(RenderList(allowList.DotNetTypeNames, "t", parameters))
            .Append(')');

        if (allowList.UpcastSourceAliases.Count > 0)
        {
            sql.Append(" OR type IN (")
                .Append(RenderList(allowList.UpcastSourceAliases, "u", parameters))
                .Append(')');
        }

        sql.Append(')');

        var rendered = new RenderedTypeFilter(allowList, sql.ToString(), parameters);
        _typeFilter = rendered;
        return rendered;
    }

    /// <summary>
    ///     The contents of one <c>IN (...)</c>: a parameter per name up to the threshold, otherwise one
    ///     JSON array expanded server-side.
    /// </summary>
    internal static string RenderList(IReadOnlyList<string> names, string prefix,
        List<EventPageParameter> parameters)
    {
        if (names.Count <= JsonValueList.Threshold)
        {
            var markers = new StringBuilder();
            for (var i = 0; i < names.Count; i++)
            {
                if (i > 0) markers.Append(", ");
                var name = $"@{prefix}{i}";
                markers.Append(name);
                parameters.Add(new EventPageParameter(name, names[i], DbType.AnsiString));
            }

            return markers.ToString();
        }

        var arrayName = $"@{prefix}_json";
        parameters.Add(new EventPageParameter(arrayName, WriteJsonArray(names), DbType.String));
        return $"SELECT value FROM OPENJSON({arrayName}) WITH (value varchar(500) '$')";
    }

    // Written by hand, like Polecat's other id lists, so this stays trim- and AOT-clean.
    private static string WriteJsonArray(IReadOnlyList<string> names)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var name in names)
            {
                writer.WriteStringValue(name);
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private sealed record RenderedTypeFilter(
        EventTypeAllowList AllowList,
        string Sql,
        IReadOnlyList<EventPageParameter> Parameters);
}
