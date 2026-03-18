using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

// Index test types
public class IndexedProduct
{
    public Guid Id { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public int Price { get; set; }
}

[Collection("integration")]
public class document_index_tests : IntegrationContext
{
    public document_index_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task CleanTable(string schema)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"IF OBJECT_ID('[{schema}].[pc_doc_indexedproduct]', 'U') IS NOT NULL DELETE FROM [{schema}].[pc_doc_indexedproduct]";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task create_single_property_index()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_single";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Sku);
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-001", Category = "Tools" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        // Verify index exists
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_indexedproduct_sku'
              AND object_id = OBJECT_ID('[idx_single].[pc_doc_indexedproduct]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task create_unique_index()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_unique";
            opts.Schema.For<IndexedProduct>()
                .UniqueIndex(x => x.Email);
        });

        await CleanTable("idx_unique");

        var p1 = new IndexedProduct { Id = Guid.NewGuid(), Sku = "A", Email = $"test-{Guid.NewGuid()}@example.com" };
        theSession.Store(p1);
        await theSession.SaveChangesAsync();

        // Verify unique index exists
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT is_unique FROM sys.indexes
            WHERE name = 'ux_pc_doc_indexedproduct_email'
              AND object_id = OBJECT_ID('[idx_unique].[pc_doc_indexedproduct]')
            """;
        var isUnique = await cmd.ExecuteScalarAsync();
        isUnique.ShouldNotBeNull();
        ((bool)isUnique).ShouldBeTrue();
    }

    [Fact]
    public async Task unique_index_rejects_duplicates()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_unique_dup";
            opts.Schema.For<IndexedProduct>()
                .UniqueIndex(x => x.Email);
        });

        await CleanTable("idx_unique_dup");

        var uniqueEmail = $"dupe-{Guid.NewGuid()}@example.com";
        var p1 = new IndexedProduct { Id = Guid.NewGuid(), Sku = "A", Email = uniqueEmail };
        theSession.Store(p1);
        await theSession.SaveChangesAsync();

        // Second insert with same email should fail
        await using var session2 = theStore.LightweightSession();
        var p2 = new IndexedProduct { Id = Guid.NewGuid(), Sku = "B", Email = uniqueEmail };
        session2.Store(p2);

        await Should.ThrowAsync<Exception>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task create_composite_index()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_composite";
            opts.Schema.For<IndexedProduct>()
                .Index(x => new { x.Category, x.Sku });
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-002", Category = "Hardware" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_indexedproduct_category_sku'
              AND object_id = OBJECT_ID('[idx_composite].[pc_doc_indexedproduct]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task create_index_with_custom_name()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_custom_name";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Category, idx => idx.IndexName = "my_custom_index");
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-003", Category = "Plumbing" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'my_custom_index'
              AND object_id = OBJECT_ID('[idx_custom_name].[pc_doc_indexedproduct]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task create_filtered_index()
    {
        // SQL Server filtered indexes cannot reference computed columns,
        // so use a regular column like tenant_id in the predicate
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_filtered3";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Sku, idx =>
                {
                    idx.Predicate = "tenant_id <> 'EXCLUDED'";
                });
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-004", Category = "Electronics" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT has_filter FROM sys.indexes
            WHERE name = 'ix_pc_doc_indexedproduct_sku'
              AND object_id = OBJECT_ID('[idx_filtered3].[pc_doc_indexedproduct]')
            """;
        var hasFilter = await cmd.ExecuteScalarAsync();
        hasFilter.ShouldNotBeNull();
        ((bool)hasFilter).ShouldBeTrue();
    }

    [Fact]
    public async Task create_index_with_numeric_sql_type()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_numeric";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Price, idx => idx.SqlType = "int");
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-005", Price = 99 };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_indexedproduct_price'
              AND object_id = OBJECT_ID('[idx_numeric].[pc_doc_indexedproduct]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task index_is_idempotent_on_repeated_ensure()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_idempotent";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Sku);
        });

        var p1 = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-A" };
        theSession.Store(p1);
        await theSession.SaveChangesAsync();

        // Create a second store with same config — should not fail
        var opts2 = new StoreOptions
        {
            ConnectionString = theStore.Options.ConnectionString,
            AutoCreateSchemaObjects = JasperFx.AutoCreate.All,
            DatabaseSchemaName = "idx_idempotent"
        };
        opts2.Schema.For<IndexedProduct>().Index(x => x.Sku);
        using var store2 = new DocumentStore(opts2);
        await using var session2 = store2.LightweightSession();
        var p2 = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-B" };
        session2.Store(p2);
        await session2.SaveChangesAsync();
    }

    [Fact]
    public async Task per_tenant_index_includes_tenant_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_tenant2";
            opts.Schema.For<IndexedProduct>()
                .UniqueIndex(x => x.Email, idx => idx.TenancyScope = TenancyScope.PerTenant);
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "T1", Email = $"tenant-{Guid.NewGuid()}@example.com" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.index_columns ic
            JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE i.name = 'ux_pc_doc_indexedproduct_email'
              AND i.object_id = OBJECT_ID('[idx_tenant2].[pc_doc_indexedproduct]')
              AND ic.is_included_column = 0
            """;
        var colCount = (int)(await cmd.ExecuteScalarAsync())!;
        colCount.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task multiple_indexes_on_same_type()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_multi2";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Sku)
                .Index(x => x.Category)
                .UniqueIndex(x => x.Email);
        });

        await CleanTable("idx_multi2");

        var product = new IndexedProduct
        {
            Id = Guid.NewGuid(), Sku = "SKU-M", Category = "Multi", Email = $"multi-{Guid.NewGuid()}@test.com"
        };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE object_id = OBJECT_ID('[idx_multi2].[pc_doc_indexedproduct]')
              AND name IN ('ix_pc_doc_indexedproduct_sku', 'ix_pc_doc_indexedproduct_category', 'ux_pc_doc_indexedproduct_email')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(3);
    }
}
