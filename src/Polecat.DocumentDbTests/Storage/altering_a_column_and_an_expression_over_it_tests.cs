using Microsoft.Data.SqlClient;
using Polecat.TestUtils;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     weasel#668/#669 — a migration that adds a column and, in the same delta, an object whose
///     <b>expression</b> names that column.
/// </summary>
/// <remarks>
///     <para>
///         SQL Server compiles a whole batch before running any of it, and binds column names against
///         tables that already exist at compile time — deferred name resolution covers only tables that
///         do not exist yet. So while <c>TableDelta.WriteUpdate</c> wrote the missing columns'
///         <c>ALTER TABLE … ADD</c> and everything after it into one batch, a later statement naming a
///         column the same delta was adding failed to compile with error 207, and <b>the <c>ALTER</c>
///         never ran either</b> — nothing in a batch that did not compile does.
///     </para>
///     <para>
///         ⚠️ <b>The line is expression versus name list, and that is why this hid for so long.</b> An
///         index's key columns, its <c>INCLUDE</c> list and a foreign key's columns are name lists and
///         always worked — which is exactly what <c>DocumentTable.AddDeclaredSchemaObjects</c> had
///         measured when it recorded that one batch is fine and no <c>GO</c> is needed. A filtered
///         index's <em>predicate</em> is an expression, and it did not.
///     </para>
///     <para>
///         Polecat declares filtered indexes, so this was reachable: any configuration change that adds
///         an ordinary column and a filtered index naming it at once. Note it has to be an
///         <em>ordinary</em> column — SQL Server refuses a filtered index predicate over a computed
///         column outright — which is why this uses soft delete's <c>is_deleted</c> rather than one of
///         the <c>cc_</c> columns.
///     </para>
///     <para>
///         The existing <c>document_index_tests.create_filtered_index</c> cannot catch this: it builds
///         the index <em>with</em> the table, where the column is created by the <c>CREATE TABLE</c> and
///         <c>WriteUpdate</c> never runs. Two phases against one schema is the whole point of this test.
///     </para>
/// </remarks>
public class altering_a_column_and_an_expression_over_it_tests: OneOffConfigurationsContext
{
    public class Ticket
    {
        public Guid Id { get; set; }
        public string Reference { get; set; } = string.Empty;
    }

    private const string TableName = "pc_doc_ticket";
    private const string IndexName = "ix_ticket_live_reference";

    [Fact]
    public async Task a_filtered_index_can_name_a_column_the_same_delta_adds()
    {
        // Phase 1 — a plain table. Not soft-deleted, so no is_deleted column, and no index.
        ConfigureStore(opts => opts.Schema.For<Ticket>());
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var before = await ColumnNamesAsync();
        before.ShouldNotBeEmpty("Phase 1 did not create the table, so the rest of this proves nothing.");
        before.ShouldNotContain("is_deleted");

        // Phase 2 — soft delete AND a filtered index whose predicate names is_deleted. ONE delta now
        // adds the column and creates an index whose expression binds against it.
        ConfigureStore(opts =>
        {
            opts.Policies.AllDocumentsSoftDeleted();
            opts.Schema.For<Ticket>().Index(x => x.Reference, idx =>
            {
                idx.IndexName = IndexName;
                idx.Predicate = "is_deleted = 0";
            });
        });

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Before weasel#669 this threw "Invalid column name 'is_deleted'" and left the table exactly as
        // phase 1 made it -- so BOTH of these assertions are the fix, not just the index one.
        (await ColumnNamesAsync()).ShouldContain("is_deleted");
        (await FilteredIndexNamesAsync()).ShouldContain(IndexName);

        // And it reaches a fixed point, so the batch separators did not turn one migration into a
        // permanent one.
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();
    }

    private async Task<List<string>> ColumnNamesAsync()
        => await NamesAsync($"""
            SELECT c.name FROM sys.columns c
            WHERE c.object_id = OBJECT_ID('{theStore.Options.DatabaseSchemaName}.{TableName}');
            """);

    /// <summary>
    ///     The index names on the table that actually carry a filter — <c>has_filter</c> rather than
    ///     mere existence, because an index created without its predicate would be the wrong object
    ///     under the right name.
    /// </summary>
    private async Task<List<string>> FilteredIndexNamesAsync()
        => await NamesAsync($"""
            SELECT i.name FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID('{theStore.Options.DatabaseSchemaName}.{TableName}')
              AND i.name IS NOT NULL AND i.has_filter = 1;
            """);

    private static async Task<List<string>> NamesAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }
}
