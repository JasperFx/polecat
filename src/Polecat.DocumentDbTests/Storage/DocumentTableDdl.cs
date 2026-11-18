using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Tests.Storage;

/// <summary>
///     Render the DDL a <see cref="DocumentMapping" /> produces, by asking the modeled table for it.
/// </summary>
/// <remarks>
///     #684: the tests that assert on generated index and foreign key DDL used to call
///     <c>DocumentIndex.ToDdlStatements(mapping)</c> and its siblings. Those methods are gone —
///     the objects are declared on <c>DocumentTable</c> now, so Weasel renders them, and a second
///     renderer in Polecat would be a second description of the schema to keep in sync.
///     <para>
///     The tests kept their intent and moved onto this: they still assert "this declaration produces
///     this SQL", and now they assert it against the only thing that produces SQL. It is a stronger
///     assertion than before, because a declaration that never reached the table would previously
///     still render here and now renders nothing.
///     </para>
/// </remarks>
internal static class DocumentTableDdl
{
    public static string RenderFor(DocumentMapping mapping)
    {
        var writer = new StringWriter();
        new DocumentTable(mapping).WriteCreateStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }
}
