using Polecat.Linq;
using Weasel.SqlServer.Tables;
using Polecat.Storage;
using Polecat.TestUtils;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// Native SQL Server 2025 JSON indexes (CREATE JSON INDEX) over the json `data` column — one index
/// covering multiple JSON paths, gated on UseNativeJsonType.
/// </summary>
public class json_index_tests : OneOffConfigurationsContext
{
    public class Doc
    {
        public Guid Id { get; set; }
        public string ServiceName { get; set; } = string.Empty;
        public DateTimeOffset BucketEnd { get; set; }
        public List<string> Tags { get; set; } = new();
    }

    private const string Table = "pc_doc_doc";
    private string Schema => GetType().Name.ToLowerInvariant();

    // ---- unit: the DECLARED schema object and its DDL (no DB) -----------------------------------
    //
    // #685 moved the CREATE JSON INDEX grammar out of JsonIndex.ToDdlStatements and into Weasel's
    // JsonIndexDefinition, declared on DocumentTable. These four facts assert the same things they
    // always did, now through the object that owns them — which also covers the declaration wiring
    // that the old renderer-only assertions could not see.

    private static DocumentTable TableFor(JsonIndex index, bool nativeJson = true)
    {
        var options = new StoreOptions { DatabaseSchemaName = "s", UseNativeJsonType = nativeJson };
        options.Schema.For<Doc>();
        var mapping = new DocumentMapping(typeof(Doc), options);
        mapping.JsonIndexes.Add(index);
        return new DocumentTable(mapping);
    }

    private static JsonIndexDefinition DeclaredIndex(JsonIndex index)
    {
        var table = TableFor(index);
        return table.Indexes.OfType<JsonIndexDefinition>().Single();
    }

    [Fact]
    public void ddl_for_multiple_paths()
    {
        var table = TableFor(new JsonIndex(["$.serviceName", "$.bucketEnd"]));
        var declared = table.Indexes.OfType<JsonIndexDefinition>().Single();

        // Weasel brackets an identifier only where one is needed, where Polecat's own renderer
        // bracketed unconditionally. Same object, different text — recorded here because the rendered
        // DDL in a generated script does change shape for ordinary names.
        declared.ToDDL(table).ShouldContain(
            "CREATE JSON INDEX jidx_pc_doc_doc ON s.pc_doc_doc (data) FOR ('$.serviceName', '$.bucketEnd')");

        // SET QUOTED_IDENTIFIER ON is required by CREATE JSON INDEX and now lives on the emission
        // path (WriteCreateStatement) rather than in the compared rendering — adding a preamble to
        // ToDDL would make every index report drift, since that text is what the delta canonicalizes.
        var writer = new StringWriter();
        declared.WriteCreateStatement(table, writer);
        writer.ToString().ShouldContain("SET QUOTED_IDENTIFIER ON");
    }

    [Fact]
    public void ddl_for_whole_document_omits_the_for_clause()
    {
        var table = TableFor(new JsonIndex([]));
        var declared = table.Indexes.OfType<JsonIndexDefinition>().Single();

        declared.ToDDL(table).ShouldContain("CREATE JSON INDEX jidx_pc_doc_doc ON s.pc_doc_doc (data)");
        declared.ToDDL(table).ShouldNotContain("FOR (");
    }

    [Fact]
    public void ddl_with_options()
    {
        var table = TableFor(new JsonIndex(["$.tags"]) { OptimizeForArraySearch = true, FillFactor = 80 });
        var declared = table.Indexes.OfType<JsonIndexDefinition>().Single();

        declared.ToDDL(table).ShouldContain("WITH (OPTIMIZE_FOR_ARRAY_SEARCH = ON, FILLFACTOR = 80)");
    }

    [Fact]
    public void declaring_a_json_index_without_native_json_throws()
    {
        // The check moved with the grammar: DocumentTable refuses at DECLARATION time rather than the
        // renderer refusing at first use, so the store cannot be built with a JSON index it could
        // never create against nvarchar(max) storage.
        Should.Throw<InvalidOperationException>(() => TableFor(new JsonIndex([]), nativeJson: false))
            .Message.ShouldContain("native json");
    }

    // ---- integration: actually create the index (native json only) ------------------------------

    [RequiresNativeJsonFact(true)]
    public async Task creates_a_json_index_covering_the_given_paths()
    {
        ConfigureStore(opts =>
            opts.Schema.For<Doc>().JsonIndex(x => new { x.ServiceName, x.BucketEnd }));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc", BucketEnd = DateTimeOffset.UtcNow });
            await session.SaveChangesAsync();
        }

        (await JsonIndexCountAsync()).ShouldBe(1);
    }

    [RequiresNativeJsonFact(true)]
    public async Task whole_document_json_index_is_created()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().JsonIndex());

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc" });
            await session.SaveChangesAsync();
        }

        (await JsonIndexCountAsync()).ShouldBe(1);
    }

    [RequiresNativeJsonFact(true)]
    public async Task optimize_for_array_search_is_applied()
    {
        ConfigureStore(opts =>
            opts.Schema.For<Doc>().JsonIndex(x => x.Tags, idx => idx.OptimizeForArraySearch = true));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), Tags = new List<string> { "a", "b" } });
            await session.SaveChangesAsync();
        }

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT ji.optimize_for_array_search
            FROM sys.json_indexes ji
            WHERE ji.object_id = OBJECT_ID('[{Schema}].[{Table}]');
            """;
        (await cmd.ExecuteScalarAsync()).ShouldBe(true);
    }

    [RequiresNativeJsonFact(true)]
    public async Task queries_return_correct_rows_with_a_json_index_present()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().JsonIndex(x => x.ServiceName));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc-A" });
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc-A" });
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc-B" });
            await session.SaveChangesAsync();
        }

        await using var session2 = theStore.QuerySession();
        var a = await session2.Query<Doc>().Where(x => x.ServiceName == "svc-A").ToListAsync();
        a.Count.ShouldBe(2);
    }

    [RequiresNativeJsonFact(true)]
    public async Task re_ensuring_is_idempotent()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().JsonIndex(x => x.ServiceName));

        for (var i = 0; i < 2; i++)
        {
            await using var session = theStore.LightweightSession();
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = $"svc-{i}" });
            await session.SaveChangesAsync();
        }

        (await JsonIndexCountAsync()).ShouldBe(1); // still exactly one, no duplicate-create error
    }

    private async Task<int> JsonIndexCountAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT COUNT(*) FROM sys.json_indexes WHERE object_id = OBJECT_ID('[{Schema}].[{Table}]');
            """;
        return (int)(await cmd.ExecuteScalarAsync())!;
    }
}
