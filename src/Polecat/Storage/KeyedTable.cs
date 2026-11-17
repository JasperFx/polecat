using System.Data.Common;
using Weasel.Core;
using Weasel.SqlServer.Tables;

namespace Polecat.Storage;

/// <summary>
///     #935: a table of Polecat's own, whose delta refuses to narrow an <c>nvarchar</c> key or metadata
///     column back to <c>varchar</c>. See <see cref="UnicodeKeys.AssertNotNarrowing" />.
/// </summary>
internal class KeyedTable : Table
{
    public KeyedTable(DbObjectName name) : base(name)
    {
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader,
        CancellationToken ct = default)
    {
        var delta = await base.CreateDeltaAsync(reader, ct).ConfigureAwait(false);
        if (delta is TableDelta tableDelta)
        {
            UnicodeKeys.AssertNotNarrowing(this, tableDelta.Actual);
        }

        return delta;
    }
}
