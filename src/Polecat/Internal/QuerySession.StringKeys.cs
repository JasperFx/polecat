using System.Text;
using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     #919: a string id or stream key the <c>varchar</c> column can't hold can't have been stored, so a
///     read of one finds nothing and doesn't ask. Bound as it is, it would find the row it collapses onto
///     instead: <c>東京</c> is <c>??</c> by the time it reaches the column, and so was <c>大阪</c>.
/// </summary>
internal partial class QuerySession
{
    // What a parameter is encoded with, then what the column keeps. A table that isn't there yet will take
    // the default. Asked of this session's own connection, so it's the right database and needs nothing
    // from the pool, and a transaction doesn't get in its way.
    private const string KeyColumnCodePagesSql = """
        DECLARE @default nvarchar(128) = CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'));
        SELECT CONVERT(int, COLLATIONPROPERTY(@default, 'CodePage')),
               CONVERT(int, COLLATIONPROPERTY(COALESCE(
                   (SELECT c.collation_name FROM sys.columns c
                    WHERE c.object_id = OBJECT_ID(@table) AND c.name = 'id'), @default), 'CodePage'));
        """;

    /// <summary>
    ///     Could a row of this document type have this id? Only a string asks, and ASCII answers itself.
    /// </summary>
    internal ValueTask<bool> CouldHaveIdAsync(DocumentMapping mapping, object id, CancellationToken token)
        => mapping.StringKeyOf(id) is { } key && !Ascii.IsValid(key)
            ? fitsAsync(mapping.QualifiedTableName, key, token)
            : new ValueTask<bool>(true);

    /// <summary>
    ///     Could a stream have this identity?
    /// </summary>
    internal ValueTask<bool> CouldHaveStreamAsync(object streamId, CancellationToken token)
        => streamId is string key && !Ascii.IsValid(key)
            ? fitsAsync(_eventGraph.StreamsTableName, key, token)
            : new ValueTask<bool>(true);

    private async ValueTask<bool> fitsAsync(string table, string key, CancellationToken token)
        => await CodePageLosingAsync(table, key, token) == null;

    /// <summary>
    ///     The code page that would change <paramref name="key" /> on its way into the id column of
    ///     <paramref name="table" />, or null if it would survive. One round trip for a table, the first time
    ///     anything outside ASCII meets it, and the answer lasts as long as the store.
    /// </summary>
    internal async ValueTask<int?> CodePageLosingAsync(string table, string key, CancellationToken token)
    {
        if (Ascii.IsValid(key)) return null;

        var column = _tableEnsurer.KeyColumnFor(table);
        if (column == null)
        {
            await using var cmd = new SqlCommand(KeyColumnCodePagesSql);
            cmd.Parameters.AddVarChar("@table", table);

            await using var reader = await ExecuteReaderAsync(cmd, token);
            await reader.ReadAsync(token);
            column = _tableEnsurer.RememberKeyColumn(table, new VarcharKeyColumn(
                await reader.IsDBNullAsync(0, token) ? 0 : reader.GetInt32(0),
                await reader.IsDBNullAsync(1, token) ? 0 : reader.GetInt32(1)));
        }

        return column.CodePageLosing(key);
    }

    internal async ValueTask<List<object>> IdsThatCouldBeStoredAsync(DocumentMapping mapping, List<object> ids,
        CancellationToken token)
    {
        // The usual answer, without a second list
        if (ids.All(x => mapping.StringKeyOf(x) is not { } key || Ascii.IsValid(key))) return ids;

        var kept = new List<object>(ids.Count);
        foreach (var id in ids)
        {
            if (await CouldHaveIdAsync(mapping, id, token)) kept.Add(id);
        }

        return kept;
    }
}
