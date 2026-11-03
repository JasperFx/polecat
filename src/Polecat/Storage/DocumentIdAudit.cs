using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polly;

namespace Polecat.Storage;

/// <summary>
///     Finds the documents whose stored JSON id no longer matches their <c>id</c> column. See
///     <see cref="AdvancedOperations.FindDocumentsWithMismatchedIdsAsync{T}(CancellationToken)" /> for why
///     they exist.
/// </summary>
/// <remarks>
///     <para>
///         <b>The comparison runs on the server and a page at a time.</b> No body comes back, only the key
///         and the member of a row that differs, so a large table costs the client nothing. Each page is
///         its own command with <see cref="StoreOptions.CommandTimeout" /> to finish, and goes through the
///         resilience pipeline by itself, so a transient failure repeats one page rather than the table,
///         and no scan holds a lock for long.
///     </para>
///     <para>
///         <c>DISTINCT</c> because the primary key of a partitioned table carries the partition column, so
///         one id can be two rows, and the join back to the table would report each of them twice.
///     </para>
///     <para>
///         <b>A row the server can't read halves the page until it is alone in one.</b> The same bug can
///         leave the member itself corrupt, and <c>JSON_VALUE</c> then raises 13643 and takes its whole page
///         with it. That row is exactly what this is looking for, so it is reported rather than thrown, at
///         a cost of a few commands. The page never selects <c>data</c>: on such a row that doesn't raise an
///         error, it drops the connection.
///     </para>
///     <para>
///         <c>JSON_PATH_EXISTS</c> because a document type can leave its id out of the body altogether, with
///         <c>[JsonIgnore]</c> or a converter. Such a document has nothing to disagree with its column, and
///         the damage this looks for overwrites a member, it doesn't remove one.
///     </para>
/// </remarks>
internal static class DocumentIdAudit
{
    /// <summary>
    ///     How many rows one command compares. A page has <see cref="StoreOptions.CommandTimeout" /> to
    ///     finish, and comparing a row means parsing its body.
    /// </summary>
    internal const int DefaultBatchSize = 5000;

    /// <summary>"Encountered corrupted JSON type".</summary>
    private const int CorruptedJson = 13643;

    /// <summary>The last key of a page: where the next one starts.</summary>
    private readonly record struct Key(object Id, object? Tenant);

    internal static async Task FindAsync(string connectionString, DocumentMapping mapping, int batchSize,
        int commandTimeout, ResiliencePipeline resilience, List<DocumentIdMismatch> found, CancellationToken token)
    {
        // Read-only, so a table that was never created is an empty answer rather than something to create.
        var exists = await resilience.ExecuteAsync(static async (state, ct) =>
        {
            await using var conn = new SqlConnection(state.connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT OBJECT_ID(@table, 'U');";
            cmd.Parameters.Add(new SqlParameter("@table", SqlDbType.NVarChar, 512) { Value = state.mapping.QualifiedTableName });
            return await cmd.ExecuteScalarAsync(ct) is not (null or DBNull);
        }, (connectionString, mapping), token);

        if (!exists) return;

        var database = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
        var sql = PageSql(mapping);

        Key? last = null;
        var size = batchSize;

        while (true)
        {
            var (next, page, unreadable) = await resilience.ExecuteAsync(static async (state, ct) =>
            {
                await using var conn = new SqlConnection(state.connectionString);
                await conn.OpenAsync(ct);
                return await ReadPageAsync(conn, state.mapping, state.sql, state.size, state.commandTimeout,
                    state.last, state.database, ct);
            }, (connectionString, mapping, sql, size, commandTimeout, last, database), token);

            if (next is not { } key) return;

            if (!unreadable)
            {
                found.AddRange(page);
                last = key;
                size = Math.Min(batchSize, size * 2);
            }
            else if (size > 1)
            {
                size /= 2;
            }
            else
            {
                found.Add(new DocumentIdMismatch(mapping.DocumentType, mapping.WrapId(key.Id), null,
                    key.Tenant as string, database, IsCorrupted: true));
                last = key;
                size = batchSize;
            }
        }
    }

    private static async ValueTask<(Key? Last, List<DocumentIdMismatch> Page, bool Unreadable)> ReadPageAsync(
        SqlConnection conn, DocumentMapping mapping, (string First, string Next) sql, int batchSize, int commandTimeout,
        Key? last, string database, CancellationToken token)
    {
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = last == null ? sql.First : sql.Next;
        cmd.CommandTimeout = commandTimeout;
        cmd.Parameters.Add(new SqlParameter("@batch", SqlDbType.Int) { Value = batchSize });

        if (last is { } key)
        {
            // A varchar id bound as nvarchar would put CONVERT_IMPLICIT on the key column and scan (#363).
            cmd.Parameters.Add(key.Id is string
                ? new SqlParameter("@lastId", SqlDbType.VarChar, 250) { Value = key.Id }
                : new SqlParameter("@lastId", key.Id));

            if (conjoined)
            {
                cmd.Parameters.Add(new SqlParameter("@lastTenant", SqlDbType.VarChar, 250) { Value = key.Tenant });
            }
        }

        var page = new List<DocumentIdMismatch>();

        await using var reader = await cmd.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token)) return (null, page, false);

        var next = new Key(reader.GetValue(0), conjoined ? reader.GetValue(1) : null);

        try
        {
            await reader.NextResultAsync(token);
            while (await reader.ReadAsync(token))
            {
                page.Add(new DocumentIdMismatch(
                    mapping.DocumentType,
                    mapping.WrapId(reader.GetValue(0)),
                    await reader.IsDBNullAsync(1, token) ? null : reader.GetString(1),
                    conjoined ? reader.GetString(2) : null,
                    database));
            }
        }
        catch (SqlException e) when (e.Number == CorruptedJson)
        {
            return (next, page, true);
        }

        return (next, page, false);
    }

    private static (string First, string Next) PageSql(DocumentMapping mapping)
    {
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;
        var table = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, mapping.TableName);
        var idType = DocumentTable.SqlTypeForIdentity(mapping.InnerIdType);

        var idPath = PathLiteral(SerializedNames.For(mapping.IdMember, mapping.StoreOptions));
        var idMember = $"JSON_VALUE(d.data, {idPath})";

        // A wrapper with no converter of its own is written as an object, { "value": ... }.
        var idValue = mapping.ValueTypeId is { } wrapper
            ? $"COALESCE({idMember}, JSON_VALUE(d.data, {PathLiteral(SerializedNames.For(mapping.IdMember, mapping.StoreOptions), SerializedNames.For(wrapper.ValueProperty, mapping.StoreOptions))}))"
            : idMember;

        var stored = SqlText.StoredValue(idMember);
        var mismatch = $"JSON_PATH_EXISTS(d.data, {idPath}) = 1 AND CASE WHEN {MatchesColumn(mapping, idValue)} THEN 0 ELSE 1 END = 1";

        string Page(string? after) => conjoined
            ? $"""
               DECLARE @keys TABLE (ord int IDENTITY(1,1) NOT NULL, tenant_id varchar(250) NOT NULL, id {idType} NOT NULL);
               INSERT INTO @keys (tenant_id, id)
               SELECT DISTINCT TOP (@batch) tenant_id, id FROM {table} {after} ORDER BY tenant_id, id;
               SELECT TOP (1) id, tenant_id FROM @keys ORDER BY ord DESC;
               SELECT d.id, {stored}, d.tenant_id FROM {table} d
               INNER JOIN @keys k ON k.tenant_id = d.tenant_id AND k.id = d.id
               WHERE {mismatch};
               """
            : $"""
               DECLARE @keys TABLE (ord int IDENTITY(1,1) NOT NULL, id {idType} NOT NULL);
               INSERT INTO @keys (id)
               SELECT DISTINCT TOP (@batch) id FROM {table} {after} ORDER BY id;
               SELECT TOP (1) id FROM @keys ORDER BY ord DESC;
               SELECT d.id, {stored} FROM {table} d
               INNER JOIN @keys k ON k.id = d.id
               WHERE {mismatch};
               """;

        return (Page(null), Page(conjoined
            ? "WHERE tenant_id > @lastTenant OR (tenant_id = @lastTenant AND id > @lastId)"
            : "WHERE id > @lastId"));
    }

    /// <summary>
    ///     The stored member compared as the column's type, so a difference of spelling isn't reported. A
    ///     member that doesn't convert is null, and a null compares as a mismatch.
    /// </summary>
    private static string MatchesColumn(DocumentMapping mapping, string idValue)
    {
        var type = mapping.InnerIdType;

        if (type == typeof(Guid)) return $"TRY_CONVERT(uniqueidentifier, {idValue}) = d.id";
        if (type == typeof(int) || type == typeof(long)) return $"TRY_CONVERT(bigint, {idValue}) = d.id";

        // By the characters. The column's collation would call 'ABC' and 'abc' the same id.
        return $"{SqlText.BinaryCollated(idValue)} = {SqlText.BinaryCollated("CONVERT(nvarchar(4000), d.id)")}";
    }

    private static string PathLiteral(params string[] keys)
        => "N'$" + string.Concat(keys.Select(x => $".\"{SqlEscaping.LiteralBody(x).Replace("\"", "\\\"")}\"")) + "'";
}
