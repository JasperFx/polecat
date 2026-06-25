using Polecat.Linq;
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

    // ---- unit: DDL generation (no DB) -----------------------------------------------------------

    [Fact]
    public void ddl_for_multiple_paths()
    {
        var mapping = new DocumentMapping(typeof(Doc), new StoreOptions { DatabaseSchemaName = "s" });
        var ddl = new JsonIndex(["$.serviceName", "$.bucketEnd"]).ToDdlStatements(mapping);

        ddl[0].ShouldContain(
            "CREATE JSON INDEX [jidx_pc_doc_doc] ON [s].[pc_doc_doc] (data) FOR ('$.serviceName', '$.bucketEnd')");
        ddl[0].ShouldContain("SET QUOTED_IDENTIFIER ON");
    }

    [Fact]
    public void ddl_for_whole_document_omits_the_for_clause()
    {
        var mapping = new DocumentMapping(typeof(Doc), new StoreOptions { DatabaseSchemaName = "s" });
        var ddl = new JsonIndex([]).ToDdlStatements(mapping);

        ddl[0].ShouldContain("CREATE JSON INDEX [jidx_pc_doc_doc] ON [s].[pc_doc_doc] (data);");
        ddl[0].ShouldNotContain("FOR (");
    }

    [Fact]
    public void ddl_with_options()
    {
        var mapping = new DocumentMapping(typeof(Doc), new StoreOptions { DatabaseSchemaName = "s" });
        var ddl = new JsonIndex(["$.tags"]) { OptimizeForArraySearch = true, FillFactor = 80 }
            .ToDdlStatements(mapping);

        ddl[0].ShouldContain("WITH (OPTIMIZE_FOR_ARRAY_SEARCH = ON, FILLFACTOR = 80)");
    }

    [Fact]
    public void ddl_throws_without_native_json()
    {
        var mapping = new DocumentMapping(typeof(Doc),
            new StoreOptions { DatabaseSchemaName = "s", UseNativeJsonType = false });

        Should.Throw<InvalidOperationException>(() => new JsonIndex([]).ToDdlStatements(mapping))
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
