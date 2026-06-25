using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// Two ergonomic index-DSL parity items: a params overload for composite indexes
/// (Index(x => x.A, x => x.B)) and helpers that index the real metadata columns
/// (created_at / last_modified / tenant_id). Both are plain nonclustered indexes that work on any
/// SQL Server version.
/// </summary>
public class index_params_and_metadata_tests : OneOffConfigurationsContext
{
    public class Doc
    {
        public Guid Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
    }

    private const string Table = "pc_doc_doc";
    private string Schema => GetType().Name.ToLowerInvariant();

    // ---- unit: raw-column DDL -------------------------------------------------------------------

    [Fact]
    public void metadata_index_ddl_targets_the_real_column_with_no_computed_column()
    {
        var mapping = new DocumentMapping(typeof(Doc), new StoreOptions { DatabaseSchemaName = "s" });
        var index = new DocumentIndex([]) { RawColumn = "created_at" };

        var ddl = index.ToDdlStatements(mapping);
        ddl.Length.ShouldBe(1); // no ALTER TABLE ADD computed column
        ddl[0].ShouldContain("CREATE NONCLUSTERED INDEX [ix_pc_doc_doc_created_at] ON [s].[pc_doc_doc] ([created_at])");
    }

    // ---- integration: params composite ----------------------------------------------------------

    [Fact]
    public async Task params_overload_creates_a_composite_index()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().Index(x => x.A, x => x.B));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), A = "x", B = 1 });
            await session.SaveChangesAsync();
        }

        // Both members are key columns of one index.
        (await KeyColumnsAsync("ix_pc_doc_doc_a_b")).ShouldBe(new[] { "cc_a", "cc_b" });
    }

    // ---- integration: metadata-column helpers ---------------------------------------------------

    [Fact]
    public async Task index_created_at_indexes_the_metadata_column()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().IndexCreatedAt());

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), A = "x" });
            await session.SaveChangesAsync();
        }

        (await KeyColumnsAsync("ix_pc_doc_doc_created_at")).ShouldBe(new[] { "created_at" });
    }

    [Fact]
    public async Task index_last_modified_indexes_the_metadata_column()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().IndexLastModified(idx => idx.SortOrder = SortOrder.Descending));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), A = "x" });
            await session.SaveChangesAsync();
        }

        (await KeyColumnsAsync("ix_pc_doc_doc_last_modified")).ShouldBe(new[] { "last_modified" });
    }

    private async Task<string[]> KeyColumnsAsync(string indexName)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.name = '{indexName}' AND i.object_id = OBJECT_ID('[{Schema}].[{Table}]')
              AND ic.is_included_column = 0
            ORDER BY ic.key_ordinal;
            """;
        var cols = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) cols.Add(reader.GetString(0));
        return cols.ToArray();
    }
}
