using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using JasperFx.Core;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Internal;
using Polecat.Storage;
using Weasel.Storage;

namespace Polecat.Events.Rewriting;

/// <summary>
///     Runs one <see cref="StoredEventRewrite" /> (#1034, the port of marten#5643). Everything it needs is the store
///     and the rewrite: there is no static state and no host, so it can be driven from a console, a test or an
///     agent that keeps the cursor.
/// </summary>
/// <remarks>
///     <para>
///         Rows are read with the async daemon's own hydration (<see cref="PolecatEventLoader" />), so upcasting,
///         binary payloads and the serializer are what a projection sees, and written back as an append would write
///         them today: the current type alias and .NET type, <c>data</c> through the store serializer or the
///         <c>{}</c> placeholder with the payload in <c>bdata</c> for a binary event.
///     </para>
///     <para>
///         <b>The concurrency guard.</b> Marten compares <c>xmin</c>, which SQL Server has no twin of without a
///         <c>rowversion</c> column. Each row is read with a SHA2_256 hash of what it stores (type, .NET type,
///         <c>data</c> and <c>bdata</c>), and the UPDATE only writes a row whose hash still matches, so a row another
///         transaction changed after the read is skipped and counted. The UPDATE re-evaluates that predicate once it
///         holds the row's lock, so a writer it waited for is seen.
///     </para>
///     <para>
///         <b>Comparisons are binary.</b> <c>data</c> is compared after both sides go through the column's own type,
///         because SQL Server 2025's <c>json</c> type normalizes what it stores (escapes, whitespace); without that,
///         every row would look changed on every run. Text is compared as bytes, so a change of case is a change
///         under a case-insensitive collation.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: reads events through the daemon loader and writes them through ISerializer, as appends do. Event types are preserved by EventGraph registration per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer and event deserialization are annotated RDC. AOT consumers supply a source-generator-backed ISerializer impl.")]
internal sealed class StoredEventRewriter
{
    private readonly DocumentStore _store;
    private readonly StoredEventRewrite _rewrite;
    private readonly EventGraph _events;
    private readonly IReadOnlyList<string>? _typeNames;

    public StoredEventRewriter(DocumentStore store, StoredEventRewrite rewrite)
    {
        if (!rewrite.AllTypes && rewrite.Types.Count == 0)
        {
            throw new InvalidOperationException(
                "Name the events to rewrite with IncludeType<T>(), Transform<T>() or IncludeAllTypes()");
        }

        if (rewrite.BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(rewrite.BatchSize));
        if (rewrite.MaxParallelDatabases <= 0) throw new ArgumentOutOfRangeException(nameof(rewrite.MaxParallelDatabases));
        if (rewrite.PauseBetweenBatches < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(rewrite.PauseBetweenBatches));

        _store = store;
        _rewrite = rewrite;
        _events = store.Options.EventGraph;

        if (!rewrite.AllTypes)
        {
            // The type's own alias, plus every stored name an upcaster reads into it
            var allowList = EventTypeAllowList.For(_events, rewrite.Types.Distinct().ToList());
            _typeNames = allowList.AliasesIncludingUpcastSources;
        }
    }

    public async Task<StoredEventRewriteResult> ExecuteAsync(CancellationToken token)
    {
        var (databases, tenantId) = await selectDatabasesAsync(token).ConfigureAwait(false);
        var results = new ConcurrentBag<StoredEventRewriteProgress>();

        await Parallel.ForEachAsync(databases,
            new ParallelOptions { MaxDegreeOfParallelism = _rewrite.MaxParallelDatabases, CancellationToken = token },
            async (database, ct) => results.Add(await rewriteAsync(database, tenantId, ct).ConfigureAwait(false)))
            .ConfigureAwait(false);

        return new StoredEventRewriteResult(results.OrderBy(x => x.Cursor.Database).ToList());
    }

    private async Task<(IReadOnlyList<PolecatDatabase> Databases, string? TenantId)> selectDatabasesAsync(
        CancellationToken token)
    {
        PolecatDatabase? tenantDatabase = null;
        string? tenantId = null;
        if (_rewrite.TenantId != null)
        {
            // Refuses a tenant a database-per-tenant store does not have
            tenantDatabase = _store.DatabaseForTenant(_rewrite.TenantId) ?? _store.Database;

            // With one tenant per database and no conjoined events, the database is the whole selection
            if (_events.TenancyStyle == TenancyStyle.Conjoined) tenantId = _rewrite.TenantId;
        }

        var databases = _store.Options.Tenancy is { } tenancy
            ? await tenancy.BuildDatabasesAsync(token).ConfigureAwait(false)
            : [_store.Database];

        if (_rewrite.DatabaseIdentifier == null)
        {
            return tenantDatabase == null ? (databases, tenantId) : ([tenantDatabase], tenantId);
        }

        var database = databases.FirstOrDefault(x => x.Identifier == _rewrite.DatabaseIdentifier)
                       ?? throw new ArgumentOutOfRangeException(nameof(StoredEventRewrite.ForDatabase),
                           $"Unknown database '{_rewrite.DatabaseIdentifier}'. Known: {databases.Select(x => x.Identifier).Join(", ")}");

        if (tenantDatabase != null && tenantDatabase.Identifier != database.Identifier)
        {
            throw new InvalidOperationException(
                $"Tenant '{_rewrite.TenantId}' is stored in database '{tenantDatabase.Identifier}', not '{database.Identifier}'");
        }

        return ([database], tenantId);
    }

    private async Task<StoredEventRewriteProgress> rewriteAsync(PolecatDatabase database, string? tenantId,
        CancellationToken token)
    {
        var cursor = _rewrite.ResumeCursors.TryGetValue(database.Identifier, out var resume)
            ? resume
            : new StoredEventRewriteCursor(database.Identifier, 0, "");

        long visited = 0, rewritten = 0, skipped = 0, before = 0, after = 0, unknown = 0;

        // The daemon's loader, used only for its row hydration
        var hydrator = new PolecatEventLoader(_events, _store.Options, database.ConnectionString);

        await using var conn = new SqlConnection(database.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        while (true)
        {
            token.ThrowIfCancellationRequested();

            var page = await readPageAsync(conn, hydrator, cursor, tenantId, token).ConfigureAwait(false);
            if (page.Rows > 0)
            {
                if (page.Events.Count > 0)
                {
                    var written = await writePageAsync(conn, page.Events, token).ConfigureAwait(false);
                    rewritten += written.Rewritten;
                    skipped += written.Skipped;
                    before += written.BytesBefore;
                    after += written.BytesAfter;
                }

                visited += page.Rows;
                unknown += page.Rows - page.Events.Count;
                cursor = cursor with { Sequence = page.LastSequence, TenantId = page.LastTenantId };
            }

            var completed = page.Rows < _rewrite.BatchSize;
            var progress = new StoredEventRewriteProgress(cursor, visited, rewritten, skipped, before, after,
                completed, unknown);
            if (_rewrite.OnProgress != null)
            {
                await _rewrite.OnProgress(progress, token).ConfigureAwait(false);
            }

            if (completed) return progress;

            if (_rewrite.PauseBetweenBatches > TimeSpan.Zero)
            {
                await Task.Delay(_rewrite.PauseBetweenBatches, token).ConfigureAwait(false);
            }
        }
    }

    private sealed record ReadEvent(IEvent Event, string TenantId, byte[] Hash);

    private sealed record Page(List<ReadEvent> Events, int Rows, long LastSequence, string LastTenantId);

    /// <summary>
    ///     What the concurrency guard hashes: everything the rewrite writes. Text goes in as UTF-16, the payload
    ///     columns as text and hex, separated by a character none of them carries.
    /// </summary>
    private static string storedHash(string alias) =>
        $"HASHBYTES('SHA2_256', CONCAT(CAST({alias}.type AS nvarchar(500)), NCHAR(1), " +
        $"ISNULL(CAST({alias}.dotnet_type AS nvarchar(500)), N''), NCHAR(1), " +
        $"CAST({alias}.data AS nvarchar(max)), NCHAR(1), ISNULL(CONVERT(nvarchar(max), {alias}.bdata, 2), N'')))";

    private async Task<Page> readPageAsync(SqlConnection conn, PolecatEventLoader hydrator,
        StoredEventRewriteCursor cursor, string? tenantId, CancellationToken token)
    {
        var columns = Polecat.Events.Internal.PcEventsRowReader.ComposeSelectColumnsWithAlias(_store.Options.Events, "d");
        var hashOrdinal = columns.Split(',').Length;

        await using var cmd = conn.CreateCommand();
        var sql = new StringBuilder("SELECT TOP(@limit) ").Append(columns).Append(", ").Append(storedHash("d"))
            .Append(" FROM ").Append(_events.EventsTableName).Append(" AS d WHERE ");

        var filters = new List<string>();
        if (_typeNames != null)
        {
            var parameters = new List<EventPageParameter>();
            filters.Add($"d.type IN ({SqlServerEventPagingDialect.RenderList(_typeNames, "t", parameters)})");
            foreach (var parameter in parameters)
            {
                cmd.Parameters.Add(new SqlParameter(parameter.Name, parameter.Value ?? DBNull.Value)
                {
                    DbType = parameter.DbType ?? DbType.AnsiString,
                    Size = parameter.DbType == DbType.String ? -1 : 8000
                });
            }
        }

        if (tenantId != null)
        {
            filters.Add("d.tenant_id = @tenant_id");
            cmd.Parameters.AddKey("@tenant_id", tenantId, _events.UnicodeKeys);
        }

        if (_rewrite.JsonOnly) filters.Add("d.bdata IS NULL");

        // Keyset on (seq_id, tenant_id): a sequence alone is not unique under per-tenant event partitions
        filters.Add("(d.seq_id > @seq OR (d.seq_id = @seq AND d.tenant_id > @tenant))");
        cmd.Parameters.Add(new SqlParameter("@seq", SqlDbType.BigInt) { Value = cursor.Sequence });
        cmd.Parameters.AddKey("@tenant", cursor.TenantId, _events.UnicodeKeys);
        cmd.Parameters.Add(new SqlParameter("@limit", SqlDbType.Int) { Value = _rewrite.BatchSize });

        sql.Append(filters.Join(" AND ")).Append(" ORDER BY d.seq_id, d.tenant_id;");
        cmd.CommandText = sql.ToString();

        var events = new List<ReadEvent>(_rewrite.BatchSize);
        var rows = 0;
        var lastSequence = cursor.Sequence;
        var lastTenantId = cursor.TenantId;

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            rows++;

            // Before the event itself, so the cursor moves past a row even when its event cannot be read
            lastSequence = reader.GetInt64(0);
            lastTenantId = reader.GetString(7);
            var hash = (byte[])reader.GetValue(hashOrdinal);

            // Only reachable with IncludeAllTypes. An event the application cannot read cannot be written back
            // the way it would be stored today, so it stays as it is.
            var @event = await hydrator.ReadRowAsync(reader, token).ConfigureAwait(false);
            if (@event == null) continue;

            events.Add(new ReadEvent(@event, lastTenantId, hash));
        }

        return new Page(events, rows, lastSequence, lastTenantId);
    }

    private async Task<(long Rewritten, long Skipped, long BytesBefore, long BytesAfter)> writePageAsync(
        SqlConnection conn, List<ReadEvent> page, CancellationToken token)
    {
        var json = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(json))
        {
            writer.WriteStartArray();
            foreach (var (e, tenantId, hash) in page)
            {
                var body = transform(e.Data);
                var type = body.GetType();
                var mapping = _events.EventMappingFor(type);

                writer.WriteStartObject();
                writer.WriteNumber("s", e.Sequence);
                writer.WriteString("t", tenantId);
                writer.WriteBase64String("h", hash);
                writer.WriteString("ty", mapping.EventTypeName);
                writer.WriteString("dn", mapping.DotNetTypeName);

                // The same split an append makes: a binary event keeps {} in data and its bytes in bdata
                var binary = _events.ResolveBinarySerializerFor(type);
                if (binary != null)
                {
                    writer.WriteString("d", EventGraph.JsonPlaceholderForBinaryEvent);
                    writer.WriteBase64String("b", binary.Serialize(type, body));
                }
                else
                {
                    writer.WriteString("d", _events.ToStorageJson(body));
                    writer.WriteNull("b");
                }

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        var table = _events.EventsTableName;
        var jsonType = _events.JsonColumnType;
        var key = _events.KeyText();
        var dotNetType = _events.KeyText(500);

        // Rows that would come out the same are not written. The hash check is repeated on the update itself, so a
        // row another transaction changed after the read is skipped rather than overwritten.
        var write = _rewrite.DryRun
            ? "SET @written = (SELECT COUNT(*) FROM @u WHERE changed = 1 AND unchanged_since_read = 1);"
            : $"""
               UPDATE e SET type = u.type, dotnet_type = u.dotnet, data = u.data, bdata = u.bdata
               FROM {table} AS e JOIN @u AS u ON e.seq_id = u.seq_id AND e.tenant_id = u.tenant_id
               WHERE u.changed = 1 AND {storedHash("e")} = u.h;
               SET @written = @@ROWCOUNT;
               """;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SET NOCOUNT ON;
            DECLARE @written bigint;
            DECLARE @u TABLE (seq_id bigint, tenant_id {key}, h varbinary(32), type varchar(500), dotnet {dotNetType},
                data nvarchar(max), bdata varbinary(max), changed bit NOT NULL DEFAULT 0,
                unchanged_since_read bit NOT NULL DEFAULT 0, bytes_before bigint, bytes_after bigint);

            INSERT INTO @u (seq_id, tenant_id, h, type, dotnet, data, bdata)
            SELECT seq_id, tenant_id, h, type, dotnet, data, bdata FROM OPENJSON(@page)
            WITH (seq_id bigint '$.s', tenant_id {key} '$.t', h varbinary(32) '$.h', type varchar(500) '$.ty',
                dotnet {dotNetType} '$.dn', data nvarchar(max) '$.d', bdata varbinary(max) '$.b');

            UPDATE u SET changed = 1,
                unchanged_since_read = CASE WHEN {storedHash("e")} = u.h THEN 1 ELSE 0 END,
                bytes_before = DATALENGTH(e.data) + ISNULL(DATALENGTH(e.bdata), 0),
                bytes_after = DATALENGTH(CAST(u.data AS {jsonType})) + ISNULL(DATALENGTH(u.bdata), 0)
            FROM @u AS u JOIN {table} AS e ON e.seq_id = u.seq_id AND e.tenant_id = u.tenant_id
            WHERE CAST(e.type AS varbinary(1000)) <> CAST(u.type AS varbinary(1000))
               OR ISNULL(CAST(e.dotnet_type AS varbinary(1000)), 0x00) <> ISNULL(CAST(u.dotnet AS varbinary(1000)), 0x00)
               OR CAST(CAST(e.data AS nvarchar(max)) AS varbinary(max))
                  <> CAST(CAST(CAST(u.data AS {jsonType}) AS nvarchar(max)) AS varbinary(max))
               OR (e.bdata IS NULL AND u.bdata IS NOT NULL) OR (e.bdata IS NOT NULL AND u.bdata IS NULL)
               OR e.bdata <> u.bdata;

            {write}

            SELECT @written,
                (SELECT COUNT(*) FROM @u WHERE changed = 1),
                (SELECT ISNULL(SUM(bytes_before), 0) FROM @u WHERE changed = 1 AND unchanged_since_read = 1),
                (SELECT ISNULL(SUM(bytes_after), 0) FROM @u WHERE changed = 1 AND unchanged_since_read = 1);
            """;

        cmd.Parameters.Add(new SqlParameter("@page", SqlDbType.NVarChar, -1)
        {
            Value = Encoding.UTF8.GetString(json.WrittenSpan)
        });

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        await reader.ReadAsync(token).ConfigureAwait(false);

        var rewritten = reader.GetInt64(0);
        var changed = Convert.ToInt64(reader.GetValue(1));
        return (rewritten, changed - rewritten, Convert.ToInt64(reader.GetValue(2)), Convert.ToInt64(reader.GetValue(3)));
    }

    private object transform(object data)
    {
        foreach (var (type, transform) in _rewrite.Transforms)
        {
            if (!type.IsInstanceOfType(data)) continue;

            data = transform(data) ?? throw new InvalidOperationException(
                $"The transform for {type.FullName} returned null. Return the event to keep it as it is.");
        }

        return data;
    }
}
