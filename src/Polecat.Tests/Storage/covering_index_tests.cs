using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// Covering computed-column indexes: Index(key, include: ...) carries the include members as non-key
/// INCLUDE columns so a query can be satisfied from the index alone (no key lookup). Each include
/// path gets its own persisted computed column. Works on any SQL Server (it's a standard nonclustered
/// index over computed columns), so these run on both native-json and nvarchar storage.
/// </summary>
public class covering_index_tests : OneOffConfigurationsContext
{
    public class Doc
    {
        public Guid Id { get; set; }
        public string ServiceName { get; set; } = string.Empty;
        public DateTimeOffset BucketEnd { get; set; }
        public int Count { get; set; }
    }

    private const string Table = "pc_doc_doc";
    private const string IndexName = "ix_pc_doc_doc_servicename";
    private string Schema => GetType().Name.ToLowerInvariant();

    [Fact]
    public void ddl_creates_include_columns_and_include_clause()
    {
        var mapping = new DocumentMapping(typeof(Doc), new StoreOptions { DatabaseSchemaName = "s" });
        mapping.Indexes.Add(new DocumentIndex(["$.serviceName"]) { IncludeColumns = ["$.bucketEnd", "$.count"] });

        // #684: rendered from the modeled table rather than from the index in isolation, which is a
        // stronger assertion -- a declaration that never reached the table now renders nothing at all.
        var ddl = DocumentTableDdl.RenderFor(mapping);

        // computed columns for the key and both include paths
        ddl.ShouldContain("cc_servicename AS");
        ddl.ShouldContain("cc_bucketend AS");
        ddl.ShouldContain("cc_count AS");
        // INCLUDE clause over the include columns
        ddl.ShouldContain("(cc_servicename) INCLUDE (cc_bucketend, cc_count)");
    }

    [Fact]
    public async Task covering_index_marks_only_the_include_paths_as_included()
    {
        ConfigureStore(opts =>
            opts.Schema.For<Doc>().Index(x => x.ServiceName, include: x => new { x.BucketEnd, x.Count }));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc", BucketEnd = DateTimeOffset.UtcNow, Count = 3 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT c.name, ic.is_included_column
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.name = '{IndexName}' AND i.object_id = OBJECT_ID('[{Schema}].[{Table}]');
            """;

        var included = new Dictionary<string, bool>();
        await using (var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            while (await reader.ReadAsync(TestContext.Current.CancellationToken)) included[reader.GetString(0)] = reader.GetBoolean(1);
        }

        included["cc_servicename"].ShouldBeFalse(); // key column
        included["cc_bucketend"].ShouldBeTrue();    // included
        included["cc_count"].ShouldBeTrue();        // included
    }

    [Fact]
    public async Task covering_index_still_returns_correct_rows()
    {
        ConfigureStore(opts =>
            opts.Schema.For<Doc>().Index(x => x.ServiceName, include: x => x.Count));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc-A", Count = 1 });
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc-A", Count = 2 });
            session.Store(new Doc { Id = Guid.NewGuid(), ServiceName = "svc-B", Count = 3 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session2 = theStore.QuerySession();
        var rows = await session2.Query<Doc>().Where(x => x.ServiceName == "svc-A").ToListAsync(TestContext.Current.CancellationToken);
        rows.Count.ShouldBe(2);
    }
}
