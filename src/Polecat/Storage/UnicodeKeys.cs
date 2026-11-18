using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Schema;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer.Tables;

namespace Polecat.Storage;

/// <summary>
///     #935: what <see cref="AdvancedOptions.UseUnicodeKeys" /> warns about when the store starts, and what
///     it refuses when a migration would undo it.
/// </summary>
internal static partial class UnicodeKeys
{
    /// <summary>SQL Server's limit on the key of a clustered index, in bytes.</summary>
    public const int ClusteredKeyLimit = 900;

    /// <summary>SQL Server's limit on the key of a nonclustered index, in bytes.</summary>
    public const int NonclusteredKeyLimit = 1700;

    /// <summary>
    ///     Log, once, the keys this store declares wider than SQL Server allows an index to hold: SQL Server's
    ///     warning 1945, which it gives when the table is created and then never again. Every row whose own
    ///     key is under the limit is fine, and one past it is refused with
    ///     <see cref="Exceptions.KeyTooLongException" />.
    /// </summary>
    public static void WarnAboutWideKeys(StoreOptions options, IEnumerable<DocumentMapping> documents)
    {
        if (!options.Advanced.UseUnicodeKeys) return;

        var tables = new EventStoreFeatureSchema(options).Objects.OfType<Table>()
            .Concat(documents.Select(x => (Table)new DocumentTable(x)));

        var wide = tables.SelectMany(KeysPastTheLimit).ToArray();
        if (wide.Length == 0) return;

        var logger = options.DotNetLogger
                     ?? options.LogFactory?.CreateLogger(typeof(DocumentStore))
                     ?? NullLogger.Instance;

        logger.LogWarning(
            "StoreOptions.Advanced.UseUnicodeKeys: these keys are declared wider than SQL Server allows an index "
            + "to hold (warning 1945): {Keys}. A row whose own key is longer is refused with KeyTooLongException, "
            + "so keep the string columns of each key under 450 characters together.",
            string.Join("; ", wide));
    }

    /// <summary>
    ///     The keys of <paramref name="table" /> whose declared width passes the limit for their kind of
    ///     index, as <c>table (columns): bytes</c>.
    /// </summary>
    internal static IEnumerable<string> KeysPastTheLimit(Table table)
    {
        if (table.PrimaryKeyColumns.Count > 0)
        {
            var limit = table.PrimaryKeyIsClustered == false ? NonclusteredKeyLimit : ClusteredKeyLimit;
            var bytes = WidthOf(table, table.PrimaryKeyColumns);
            if (bytes > limit)
            {
                yield return $"{table.Identifier} ({string.Join(", ", table.PrimaryKeyColumns)}): {bytes} bytes, limit {limit}";
            }
        }

        foreach (var index in table.Indexes)
        {
            if (index is not IndexDefinition { Columns: { Length: > 0 } columns } definition) continue;

            var limit = definition.IsClustered ? ClusteredKeyLimit : NonclusteredKeyLimit;
            var bytes = WidthOf(table, columns);
            if (bytes > limit)
            {
                yield return $"{table.Identifier}.{definition.Name} ({string.Join(", ", columns)}): {bytes} bytes, limit {limit}";
            }
        }
    }

    private static int WidthOf(Table table, IEnumerable<string> columns)
        => columns.Sum(x => table.ColumnFor(x) is { } column ? BytesOf(column.Type) : 0);

    /// <summary>The most bytes a value of <paramref name="sqlType" /> takes in an index key.</summary>
    internal static int BytesOf(string sqlType)
    {
        var match = SizedType().Match(sqlType.Trim());
        var name = (match.Success ? match.Groups["name"].Value : sqlType.Trim()).ToLowerInvariant();
        var size = match.Success && int.TryParse(match.Groups["size"].Value, out var parsed) ? parsed : 1;

        return name switch
        {
            "nvarchar" or "nchar" => 2 * size,
            "varchar" or "char" or "varbinary" or "binary" => size,
            "uniqueidentifier" => 16,
            "bigint" => 8,
            "int" => 4,
            "smallint" => 2,
            "tinyint" or "bit" => 1,
            "datetimeoffset" => 10,
            "datetime2" or "datetime" => 8,
            "date" => 3,
            "decimal" or "numeric" => 17,
            _ => 0
        };
    }

    [GeneratedRegex(@"^(?<name>[a-zA-Z0-9_]+)\s*\(\s*(?<size>\d+)")]
    private static partial Regex SizedType();

    /// <summary>
    ///     Refuse a delta that would narrow an <c>nvarchar</c> column of Polecat's back to <c>varchar</c>,
    ///     which is what turning <see cref="AdvancedOptions.UseUnicodeKeys" /> off on a store that had it on
    ///     would do. Every character the code page doesn't have would become <c>?</c>, with no error.
    /// </summary>
    /// <remarks>
    ///     In the delta itself, so it holds for every way a migration is made: a session's first use,
    ///     <c>ApplyAllConfiguredChangesToDatabaseAsync</c>, and the script <c>db-patch</c> writes.
    /// </remarks>
    public static void AssertNotNarrowing(Table expected, Table? actual)
    {
        if (actual == null) return;

        var narrowed = expected.Columns
            .Where(x => string.IsNullOrEmpty(x.ComputedExpression)
                        && actual.ColumnFor(x.Name) is { } existing && string.IsNullOrEmpty(existing.ComputedExpression)
                        && KeyColumns.IsUnicode(existing.Type) && !KeyColumns.IsUnicode(x.Type)
                        && x.Type.Contains("char", StringComparison.OrdinalIgnoreCase))
            .Select(x => x.Name)
            .ToArray();

        if (narrowed.Length == 0) return;

        throw new InvalidOperationException(
            $"Polecat won't change {expected.Identifier} ({string.Join(", ", narrowed)}) from nvarchar to varchar. "
            + "This store was created with StoreOptions.Advanced.UseUnicodeKeys = true, and narrowing the columns "
            + "would turn every character the database's code page doesn't have into '?' with no error. Turn "
            + "UseUnicodeKeys back on for this store.");
    }
}
