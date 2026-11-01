using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polly;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Storage.FullText;

/// <summary>
///     Re-tokenizes every document of a type by writing each row back unchanged, which fires the
///     maintenance trigger. See <see cref="AdvancedOperations.RebuildFullTextIndexAsync{T}(CancellationToken)" /> for when
///     and why.
/// </summary>
/// <remarks>
///     <para>
///         <b>Through the trigger rather than a <c>DELETE</c> and an <c>INSERT</c> of its own</b>, for two
///         reasons. It leaves one tokenizer: a second copy of the rule is exactly how the stored and the
///         searched terms drift apart. And it is safe against a concurrent writer without having to reason
///         about it, because a row's tokens are replaced under that row's own exclusive lock by the same
///         code that replaces them on any other write. A hand-written rebuild reading a document and then
///         inserting its tokens can race one, and the loser leaves stale tokens or duplicates.
///     </para>
///     <para>
///         <c>version = version</c> changes nothing a reader can see: not the version, not
///         <c>last_modified</c>, not the body. SQL Server logs little for an update that does not change
///         a value, so what this costs is the trigger's work and not a rewrite of the documents.
///     </para>
///     <para>
///         Batched by primary key so a large table is not one transaction. A document written while this
///         runs is tokenized by its own write, so missing it costs nothing. The keys are read into a
///         table variable first because a table with a trigger cannot take a plain <c>OUTPUT</c>
///         (Msg 334), and <c>UPDATE TOP</c> has no <c>ORDER BY</c> to page with.
///     </para>
/// </remarks>
internal static class FullTextIndexRebuild
{
    /// <summary>
    ///     How many documents go through the trigger in one statement, and so in one transaction.
    /// </summary>
    /// <remarks>
    ///     ⚠️ Low because a batch has <see cref="StoreOptions.CommandTimeout" /> to finish and the trigger
    ///     replaces every token of every document in it. Over documents of 5,000 characters, about a
    ///     thousand tokens each, a batch of 1,000 ran out of the default 30 seconds and a batch of 100
    ///     took a little over two.
    /// </remarks>
    internal const int DefaultBatchSize = 100;

    /// <summary>
    ///     The last key of a batch: where the next one starts.
    /// </summary>
    private readonly record struct Key(object Id, object? Tenant);

    /// <summary>
    ///     Refuses a stale trigger, then rebuilds a batch at a time. Each step goes through
    ///     <paramref name="resilience" /> by itself, so a transient failure repeats one batch rather than
    ///     the table.
    /// </summary>
    internal static async Task RebuildAsync(string connectionString, DocumentMapping mapping, int batchSize,
        int commandTimeout, ResiliencePipeline resilience, CancellationToken token)
    {
        await resilience.ExecuteAsync(static async (state, ct) =>
        {
            await using var conn = new SqlConnection(state.connectionString);
            await conn.OpenAsync(ct);
            await AssertTriggerIsCurrentAsync(conn, state.mapping, ct);
        }, (connectionString, mapping), token);

        Key? last = null;
        do
        {
            last = await resilience.ExecuteAsync(static async (state, ct) =>
            {
                await using var conn = new SqlConnection(state.connectionString);
                await conn.OpenAsync(ct);
                return await RebuildBatchAsync(conn, state.mapping, state.batchSize, state.commandTimeout,
                    state.last, ct);
            }, (connectionString, mapping, batchSize, commandTimeout, last), token);
        } while (last != null);
    }

    private static async ValueTask<Key?> RebuildBatchAsync(SqlConnection conn, DocumentMapping mapping,
        int batchSize, int commandTimeout, Key? last, CancellationToken token)
    {
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = BatchSql(mapping, conjoined, first: last == null);
        cmd.CommandTimeout = commandTimeout;
        cmd.Parameters.Add(new SqlParameter("@batch", SqlDbType.Int) { Value = batchSize });

        if (last is { } key)
        {
            cmd.Parameters.Add(IdParameter(key.Id));
            if (conjoined)
            {
                cmd.Parameters.Add(new SqlParameter("@lastTenant", SqlDbType.VarChar, 250) { Value = key.Tenant });
            }
        }

        await using var reader = await cmd.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token)) return null;

        return new Key(reader.GetValue(0), conjoined ? reader.GetValue(1) : null);
    }

    // A varchar id bound as nvarchar would put CONVERT_IMPLICIT on the key column and scan (#363).
    private static SqlParameter IdParameter(object id) =>
        id is string
            ? new SqlParameter("@lastId", SqlDbType.VarChar, 250) { Value = id }
            : new SqlParameter("@lastId", id);

    private static string BatchSql(DocumentMapping mapping, bool conjoined, bool first)
    {
        var table = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, mapping.TableName);
        var idType = DocumentTable.SqlTypeForIdentity(mapping.InnerIdType);

        return conjoined
            ? $"""
               DECLARE @keys TABLE (tenant_id varchar(250) NOT NULL, id {idType} NOT NULL);
               INSERT INTO @keys (tenant_id, id)
               SELECT TOP (@batch) tenant_id, id FROM {table}
               {(first ? "" : "WHERE tenant_id > @lastTenant OR (tenant_id = @lastTenant AND id > @lastId)")}
               ORDER BY tenant_id, id;
               UPDATE d SET version = d.version FROM {table} d
               INNER JOIN @keys k ON k.tenant_id = d.tenant_id AND k.id = d.id;
               SELECT TOP (1) id, tenant_id FROM @keys ORDER BY tenant_id DESC, id DESC;
               """
            : $"""
               DECLARE @keys TABLE (id {idType} NOT NULL);
               INSERT INTO @keys (id)
               SELECT TOP (@batch) id FROM {table}
               {(first ? "" : "WHERE id > @lastId")}
               ORDER BY id;
               UPDATE d SET version = d.version FROM {table} d INNER JOIN @keys k ON k.id = d.id;
               SELECT TOP (1) id FROM @keys ORDER BY id DESC;
               """;
    }

    /// <summary>
    ///     Refuses to run through a trigger that is not the one this version declares.
    /// </summary>
    /// <remarks>
    ///     Under <c>AutoCreate.None</c> nothing has replaced an older trigger, and a rebuild through it
    ///     would re-create the very tokens it was run to get rid of and report success.
    /// </remarks>
    private static async Task AssertTriggerIsCurrentAsync(SqlConnection conn, DocumentMapping mapping,
        CancellationToken token)
    {
        var trigger = new FullTextTrigger(mapping, mapping.FullTextIndexes);
        var migration = await SchemaMigration.DetermineAsync(conn, token, trigger);

        if (migration.Difference != SchemaPatchDifference.None)
        {
            throw new InvalidOperationException(
                $"The full-text trigger on {mapping.QualifiedTableName} is not the one this version of "
                + "Polecat declares, so rebuilding through it would store the tokens it is meant to "
                + "replace. Apply the schema change first: run ApplyAllConfiguredChangesToDatabaseAsync(), "
                + "or the script from db-patch, and rebuild again.");
        }
    }
}
