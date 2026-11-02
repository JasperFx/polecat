using System.Data.Common;
using Weasel.Core;
using Weasel.SqlServer.Tables;

namespace Polecat.Storage;

/// <summary>
///     A table Polecat declares but does not own outright: a migration creates what is missing from the
///     declaration, and never drops or churns anything the declaration does not mention.
/// </summary>
internal class AdditiveTable : Table
{
    public AdditiveTable(DbObjectName name) : base(name)
    {
    }

    /// <summary>
    ///     #267: strip objects this model does not declare out of the fetched (actual) table before the
    ///     diff, so Polecat is purely additive — it creates what is missing from its own model and never
    ///     drops or churns a column, index or foreign key it does not own. A user may add their own (a
    ///     persisted computed column plus a unique index via an EF migration, say), and Weasel's default
    ///     <see cref="TableDelta" /> would treat each as an "extra" and emit <c>DROP COLUMN</c> /
    ///     <c>DROP INDEX</c> — destructive — while also leaving a permanently non-empty diff that makes
    ///     every storage-ensure re-run DDL.
    ///     <para>
    ///     ⚠️ <b>#684 narrowed what this covers without changing a line of it, and that is the point.</b>
    ///     Polecat's own computed columns, indexes and foreign keys used to be unmodeled too — raw DDL
    ///     run by <see cref="Internal.DocumentTableEnsurer" /> — so they were stripped here alongside the
    ///     user's, which is why the generated script omitted them and
    ///     <c>AssertDatabaseMatchesConfigurationAsync</c> reported a match without them. They are
    ///     declared in the model now (see DocumentTable.DeclaredObjects.cs), so they fall on the modeled
    ///     side of these three comparisons and reconcile normally. Nothing here had to change: the
    ///     override was always "keep what I do not declare", and the fix was to declare more.
    ///     </para>
    ///     <para>
    ///     #882, #883: a flat table and <c>pc_tenants</c> were created once and never compared to anything
    ///     again, so bringing them into the model puts every existing one in front of its first delta. A
    ///     reporting table is exactly where somebody adds an index, and a plain <see cref="Table" /> would
    ///     drop it on the first start after upgrading.
    ///     </para>
    /// </summary>
    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader,
        CancellationToken ct = default)
    {
        var delta = (TableDelta)await base.CreateDeltaAsync(reader, ct).ConfigureAwait(false);

        // Null Actual => the table does not exist yet; nothing to strip (this is a Create).
        var actual = delta.Actual;
        if (actual == null)
        {
            return delta;
        }

        var modeledColumns = Columns.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var extra in actual.Columns.Where(c => !modeledColumns.Contains(c.Name)).ToList())
        {
            actual.RemoveColumn(extra.Name);
        }

        var modeledIndexes = Indexes.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var extra in actual.Indexes.Where(i => !modeledIndexes.Contains(i.Name)).ToList())
        {
            actual.Indexes.Remove(extra);
        }

        var modeledForeignKeys = ForeignKeys.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var extra in actual.ForeignKeys.Where(f => !modeledForeignKeys.Contains(f.Name)).ToList())
        {
            actual.ForeignKeys.Remove(extra);
        }

        // Recompute the diff against the now-stripped actual table.
        return new TableDelta(this, actual);
    }
}
