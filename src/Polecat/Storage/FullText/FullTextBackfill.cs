using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Storage.FullText;

/// <summary>
///     #685: the one-time backfill that makes a newly declared index cover rows written before it
///     existed, a batch of documents at a time.
/// </summary>
/// <remarks>
///     <para>
///         <b>This is a data migration, not a schema object, and it stays raw SQL for that reason.</b> The
///         token table and the trigger are Weasel objects (<see cref="FullTextTokenTable" />,
///         <see cref="FullTextTrigger" />) so they appear in a generated script, are compared by
///         <c>AssertDatabaseMatchesConfigurationAsync()</c>, and can be refused under
///         <c>AutoCreate.None</c>. A backfill is none of those things: it describes rows rather than
///         structure, there is nothing in the catalog for a delta to compare it against, and it is correct
///         to run it after the schema converges rather than as part of converging.
///     </para>
///     <para>
///         Scoped to documents this member has no tokens for, so it is idempotent and so declaring a second
///         member later does not re-tokenize the first. A trigger only fires on writes made after it
///         exists, so without this a declaration against a store that already holds documents produces an
///         index matching nothing: no error, no rows, just a search that quietly answers empty.
///     </para>
///     <para>
///         ⚠️ <b>Batched because one statement could not finish</b> (#877). Declaring an index over 8,000
///         documents of 7,000 characters timed out at 30 seconds inside the first request, rolled back, and
///         the next request began the same doomed attempt, so every query of the type failed from then on.
///         2,000 of them took 12 seconds. A batch has <see cref="StoreOptions.CommandTimeout" /> to itself
///         and what it wrote stays written, so the first request is slow rather than impossible. On an index
///         with nothing to do, this is one statement, as it always was.
///     </para>
///     <para>
///         ⚠️ <b>The list is taken once, and the batches work down it.</b> Asking the token table what is
///         missing per batch is an anti-join over a heap, and over 2,000 documents with no index on
///         <c>doc_id</c>, which a table from before #916 still has none of, that took 133 seconds where one
///         statement took 12. Asked once it costs what the one statement always did, and a document that
///         yields no tokens simply comes off the list. A batch still asks about its own hundred keys before
///         it writes them (#1021), because the list can be out of date by then; see <c>BatchSql</c>.
///     </para>
///     <para>
///         ⚠️ <b>Repairing an existing index is not this job, and it is held back from doing so</b> (#835,
///         #839). Two kinds of document are out of date after a tokenizer change. One with SOME tokens is
///         invisible here, however wrong they are: <c>"quick\nbrown fox"</c> keeps its stale
///         <c>quick\nbrown</c> until it is next written. One with NONE, like a member over 4000 characters,
///         is exactly what <c>NOT EXISTS</c> finds, and that can be far too much work for whichever request
///         happened to touch the type first.
///     </para>
///     <para>
///         So once a member has tokens, the bare <c>JSON_VALUE</c> below keeps this to what it could always
///         reach, and those documents wait for
///         <see cref="AdvancedOperations.RebuildFullTextIndexAsync{T}(CancellationToken)" />, which is something
///         an operator chooses to run. A member with no tokens at all is a new declaration or a changed
///         tokenizer, where the promise above applies and every document is covered. That is decided once,
///         before the first batch, because the first batch is what changes the answer.
///     </para>
///     <para>
///         ⚠️ <b>It does not stop for the caller's token.</b> A new index that stopped halfway has tokens,
///         so the next attempt would call it established and never reach the long documents left over. The
///         only thing cancelling could gain is a request that gives up sooner on work the next one has to do.
///         A process that dies partway leaves that state anyway, and a rebuild is what repairs it.
///     </para>
/// </remarks>
internal static class FullTextBackfill
{
    /// <summary>How many documents are tokenized in one statement, and so in one transaction.</summary>
    internal const int BatchSize = FullTextIndexRebuild.DefaultBatchSize;

    private const string Pending = "#pc_ft_backfill";

    /// <summary>
    ///     Clears tokens under a key nothing declares, then covers what each declared index is missing.
    /// </summary>
    internal static async Task RunAsync(SqlConnection conn, DocumentMapping mapping, int commandTimeout)
    {
        if (mapping.FullTextIndexes.Count == 0) return;

        var ftTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, FullTextIndex.TableNameFor(mapping));

        // #642: rows under a key no declared index produces any more -- an index whose tokenizer changed,
        // or one that was removed -- are cleared first. The rows they hold were tokenized by a rule nothing
        // searches with, and leaving them is only harmless until a key is reused.
        var declaredKeys = string.Join(", ", mapping.FullTextIndexes.Select(x => SqlEscaping.Literal(x.TokenKey)));
        await executeAsync(conn, $"DELETE FROM {ftTable} WHERE member NOT IN ({declaredKeys});", commandTimeout).ConfigureAwait(false);

        foreach (var index in mapping.FullTextIndexes)
        {
            var key = SqlEscaping.Literal(index.TokenKey);
            var established = await executeAsync(conn,
                $"SELECT CASE WHEN EXISTS (SELECT 1 FROM {ftTable} WHERE member = {key}) THEN 1 ELSE 0 END;",
                commandTimeout).ConfigureAwait(false) == 1;

            var pending = await executeAsync(conn, ListSql(mapping, index, established, ftTable), commandTimeout).ConfigureAwait(false);

            var batch = BatchSql(mapping, index, ftTable);
            while (pending > 0)
            {
                pending = await executeBatchAsync(conn, batch, commandTimeout).ConfigureAwait(false);
            }
        }

        await executeAsync(conn, $"DROP TABLE IF EXISTS {Pending};", commandTimeout).ConfigureAwait(false);
    }

    private static async Task<int> executeAsync(SqlConnection conn, string sql, int commandTimeout,
        SqlTransaction? tx = null)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = commandTimeout;
        cmd.Transaction = tx;

        if (tx != null)
        {
            cmd.Parameters.Add(new SqlParameter("@batch", SqlDbType.Int) { Value = BatchSize });
        }

        return await cmd.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false) is int value ? value : 0;
    }

    /// <summary>
    ///     One batch, in a transaction of its own: the documents it holds stay held until their tokens are
    ///     written, and a batch that fails puts its keys back on the list.
    /// </summary>
    private static async Task<int> executeBatchAsync(SqlConnection conn, string sql, int commandTimeout)
    {
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(CancellationToken.None).ConfigureAwait(false);
        var taken = await executeAsync(conn, sql, commandTimeout, tx).ConfigureAwait(false);
        await tx.CommitAsync(CancellationToken.None).ConfigureAwait(false);

        return taken;
    }

    /// <summary>
    ///     The documents this member has no tokens for, into a temp table, answering how many.
    /// </summary>
    /// <remarks>
    ///     Empty and absent members are left out, so that a table full of them is not walked a batch at a
    ///     time on every first use. <c>&lt;&gt;</c> pads, which only leaves out more of what would yield
    ///     nothing.
    /// </remarks>
    private static string ListSql(DocumentMapping mapping, FullTextIndex index, bool established, string ftTable)
    {
        var docTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, mapping.TableName);
        var keys = mapping.TenancyStyle == TenancyStyle.Conjoined ? "i.tenant_id, i.id" : "i.id";

        var text = established
            ? $"JSON_VALUE(i.data, '{index.JsonPath}')"
            : SqlText.UnlimitedJsonString("i.data", index.JsonPath, DocumentIndex.UsesNativeJson(mapping));

        return $"""
                DROP TABLE IF EXISTS {Pending};
                SELECT {keys} INTO {Pending} FROM {docTable} i
                WHERE {text} <> ''
                  AND NOT EXISTS (SELECT 1 FROM {ftTable} ft
                                  WHERE ft.doc_id = i.id AND ft.tenant_id = {FullTextIndex.TenantColumn(mapping)}
                                    AND ft.member = {SqlEscaping.Literal(index.TokenKey)});
                SELECT @@ROWCOUNT;
                """;
    }

    /// <summary>
    ///     Tokenizes the next batch off the list, answering how many that took.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A document written since the list was taken is left alone, because the trigger has already
    ///         tokenized it and doing so again would double its terms. ⚠️ <b>What says so is the token
    ///         table, not the clock</b> (#1021). This used to tokenize only <c>last_modified &lt; @started</c>,
    ///         and <c>SYSDATETIMEOFFSET()</c> ticks about every 4 ms on Linux: a document written before the
    ///         index existed, in the same tick the backfill began, carried <c>last_modified = @started</c>,
    ///         was skipped here, and had never been near the trigger either. It stayed out of every search
    ///         for good, with no error, until it was next written.
    ///     </para>
    ///     <para>
    ///         The clock is not kept even as a shortcut for documents written well before the start. Not
    ///         every write that fires the trigger moves <c>last_modified</c>: a soft delete sets only
    ///         <c>is_deleted</c> and <c>deleted_at</c>, and a hand-written <c>UPDATE</c> sets what it likes. One
    ///         of those landing between the list and the batch has been tokenized by the trigger and still
    ///         looks old, so the shortcut would tokenize it a second time. The cost of asking is a seek on
    ///         <c>doc_id</c> per document. On a token table from before #916 that index is missing and each
    ///         batch scans the table instead, until the schema is applied, which is the same thing the
    ///         trigger already does on such a table for every write.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>The asking is only an answer while the documents are held still</b>, which is what the
    ///         <c>UPDLOCK</c> is for, taken in a statement of its own so that it comes first. A write to one
    ///         of them either committed before it, with its tokens there to be seen, or waits until this
    ///         batch commits, and then the trigger deletes what this wrote before writing its own. Without it
    ///         a write could land between the look and the insert, and both would tokenize the document.
    ///         The rows are read through <c>version</c> because a count alone can be answered from another
    ///         index, which would lock nothing a writer takes.
    ///     </para>
    /// </remarks>
    private static string BatchSql(DocumentMapping mapping, FullTextIndex index, string ftTable)
    {
        var docTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, mapping.TableName);
        var idType = mapping.IdSqlType;
        var key = SqlEscaping.Literal(index.TokenKey);
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;

        var (columns, deleted, join) = conjoined
            ? ($"tenant_id {mapping.StoreOptions.KeyText()} NOT NULL, id {idType} NOT NULL", "deleted.tenant_id, deleted.id",
                "k.tenant_id = i.tenant_id AND k.id = i.id")
            : ($"id {idType} NOT NULL", "deleted.id", "k.id = i.id");

        return $"""
                DECLARE @keys TABLE ({columns});
                DECLARE @held bigint;
                DELETE TOP (@batch) FROM {Pending} OUTPUT {deleted} INTO @keys;
                SELECT @held = MAX(i.version) FROM {docTable} i WITH (UPDLOCK, ROWLOCK)
                INNER JOIN @keys k ON {join};
                INSERT INTO {ftTable} (doc_id, tenant_id, member, term, pos)
                SELECT i.id, {FullTextIndex.TenantColumn(mapping)}, {key}, {index.TermExpression(mapping)}, {FullTextIndex.PositionExpression(mapping)}
                FROM {docTable} i
                INNER JOIN @keys k ON {join}
                {index.TokenizeClause("i", DocumentIndex.UsesNativeJson(mapping))}
                AND NOT EXISTS (SELECT 1 FROM {ftTable} ft
                                WHERE ft.doc_id = i.id AND ft.tenant_id = {FullTextIndex.TenantColumn(mapping)}
                                  AND ft.member = {key});
                SELECT COUNT(*) FROM @keys;
                """;
    }
}
