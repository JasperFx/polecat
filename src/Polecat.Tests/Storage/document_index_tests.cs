using Polecat.Attributes;
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

// Attribute-based index test types
public class AttributeIndexedDoc
{
    public Guid Id { get; set; }

    [Index]
    public string Name { get; set; } = string.Empty;

    [UniqueIndex]
    public string Code { get; set; } = string.Empty;

    public int Value { get; set; }
}

public class CompositeUniqueDoc
{
    public Guid Id { get; set; }

    [UniqueIndex(IndexName = "ux_fullname")]
    public string FirstName { get; set; } = string.Empty;

    [UniqueIndex(IndexName = "ux_fullname")]
    public string LastName { get; set; } = string.Empty;
}

public class CasingAttributeDoc
{
    public Guid Id { get; set; }

    [Index(Casing = IndexCasing.Lower)]
    public string UserName { get; set; } = string.Empty;

    [UniqueIndex(Casing = IndexCasing.Upper)]
    public string Email { get; set; } = string.Empty;
}

public class CustomSqlTypeAttributeDoc
{
    public Guid Id { get; set; }

    [Index(SqlType = "int")]
    public int Score { get; set; }

    [Index(SqlType = "varchar(500)")]
    public string LongDescription { get; set; } = string.Empty;
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
            DatabaseSchemaName = "idx_idempotent",
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
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

    #region Case Transformation Tests

    [Fact]
    public async Task create_index_with_lower_casing()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_lower";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Sku, idx => idx.Casing = IndexCasing.Lower);
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "SKU-UPPER-001" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        // Verify index with casing suffix exists
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_indexedproduct_sku_lower'
              AND object_id = OBJECT_ID('[idx_lower].[pc_doc_indexedproduct]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);

        // Verify the computed column stores lower-cased value
        await using var cmd2 = conn.CreateCommand();
        cmd2.CommandText = """
            SELECT [cc_sku_lower] FROM [idx_lower].[pc_doc_indexedproduct]
            WHERE id = @id
            """;
        cmd2.Parameters.AddWithValue("@id", product.Id);
        var storedValue = (string?)(await cmd2.ExecuteScalarAsync());
        storedValue.ShouldBe("sku-upper-001");
    }

    [Fact]
    public async Task create_index_with_upper_casing()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_upper";
            opts.Schema.For<IndexedProduct>()
                .Index(x => x.Sku, idx => idx.Casing = IndexCasing.Upper);
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "sku-lower-001" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        // Verify index with casing suffix exists
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_indexedproduct_sku_upper'
              AND object_id = OBJECT_ID('[idx_upper].[pc_doc_indexedproduct]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);

        // Verify the computed column stores upper-cased value
        await using var cmd2 = conn.CreateCommand();
        cmd2.CommandText = """
            SELECT [cc_sku_upper] FROM [idx_upper].[pc_doc_indexedproduct]
            WHERE id = @id
            """;
        cmd2.Parameters.AddWithValue("@id", product.Id);
        var storedValue = (string?)(await cmd2.ExecuteScalarAsync());
        storedValue.ShouldBe("SKU-LOWER-001");
    }

    [Fact]
    public async Task create_composite_index_with_casing()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_comp_case";
            opts.Schema.For<IndexedProduct>()
                .Index(x => new { x.Category, x.Sku }, idx => idx.Casing = IndexCasing.Upper);
        });

        var product = new IndexedProduct { Id = Guid.NewGuid(), Sku = "mixed-Sku", Category = "mixed-Cat" };
        theSession.Store(product);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_indexedproduct_category_sku_upper'
              AND object_id = OBJECT_ID('[idx_comp_case].[pc_doc_indexedproduct]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task unique_index_with_lower_casing_rejects_case_insensitive_duplicates()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_uniq_lower";
            opts.Schema.For<IndexedProduct>()
                .UniqueIndex(x => x.Email, idx => idx.Casing = IndexCasing.Lower);
        });

        // Clean
        await using var cleanConn = await OpenConnectionAsync();
        await using var cleanCmd = cleanConn.CreateCommand();
        cleanCmd.CommandText = "IF OBJECT_ID('[idx_uniq_lower].[pc_doc_indexedproduct]', 'U') IS NOT NULL DELETE FROM [idx_uniq_lower].[pc_doc_indexedproduct]";
        await cleanCmd.ExecuteNonQueryAsync();

        var p1 = new IndexedProduct { Id = Guid.NewGuid(), Sku = "A", Email = "Test@Example.COM" };
        theSession.Store(p1);
        await theSession.SaveChangesAsync();

        // Second insert with different casing should fail because index is lowered
        await using var session2 = theStore.LightweightSession();
        var p2 = new IndexedProduct { Id = Guid.NewGuid(), Sku = "B", Email = "test@example.com" };
        session2.Store(p2);

        await Should.ThrowAsync<Exception>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    #endregion

    #region Attribute-Based Index Tests

    [Fact]
    public async Task attribute_index_creates_index_on_property()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_attr_basic";
        });

        var doc = new AttributeIndexedDoc { Id = Guid.NewGuid(), Name = "Test", Code = $"CODE-{Guid.NewGuid()}" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // Verify the [Index] attribute created an index on Name
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_attributeindexeddoc_name'
              AND object_id = OBJECT_ID('[idx_attr_basic].[pc_doc_attributeindexeddoc]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task attribute_unique_index_creates_unique_index()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_attr_unique";
        });

        // Clean table
        await using var cleanConn = await OpenConnectionAsync();
        await using var cleanCmd = cleanConn.CreateCommand();
        cleanCmd.CommandText = "IF OBJECT_ID('[idx_attr_unique].[pc_doc_attributeindexeddoc]', 'U') IS NOT NULL DELETE FROM [idx_attr_unique].[pc_doc_attributeindexeddoc]";
        await cleanCmd.ExecuteNonQueryAsync();

        var doc = new AttributeIndexedDoc { Id = Guid.NewGuid(), Name = "Test", Code = $"UNIQUE-{Guid.NewGuid()}" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // Verify the [UniqueIndex] attribute created a unique index on Code
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT is_unique FROM sys.indexes
            WHERE name = 'ux_pc_doc_attributeindexeddoc_code'
              AND object_id = OBJECT_ID('[idx_attr_unique].[pc_doc_attributeindexeddoc]')
            """;
        var isUnique = await cmd.ExecuteScalarAsync();
        isUnique.ShouldNotBeNull();
        ((bool)isUnique).ShouldBeTrue();
    }

    [Fact]
    public async Task attribute_unique_index_rejects_duplicates()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_attr_dup";
        });

        // Clean table
        await using var cleanConn = await OpenConnectionAsync();
        await using var cleanCmd = cleanConn.CreateCommand();
        cleanCmd.CommandText = "IF OBJECT_ID('[idx_attr_dup].[pc_doc_attributeindexeddoc]', 'U') IS NOT NULL DELETE FROM [idx_attr_dup].[pc_doc_attributeindexeddoc]";
        await cleanCmd.ExecuteNonQueryAsync();

        var code = $"DUP-{Guid.NewGuid()}";
        var doc1 = new AttributeIndexedDoc { Id = Guid.NewGuid(), Name = "First", Code = code };
        theSession.Store(doc1);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var doc2 = new AttributeIndexedDoc { Id = Guid.NewGuid(), Name = "Second", Code = code };
        session2.Store(doc2);

        await Should.ThrowAsync<Exception>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task attribute_composite_unique_index_by_index_name()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_attr_comp";
        });

        // Clean table
        await using var cleanConn = await OpenConnectionAsync();
        await using var cleanCmd = cleanConn.CreateCommand();
        cleanCmd.CommandText = "IF OBJECT_ID('[idx_attr_comp].[pc_doc_compositeuniquedoc]', 'U') IS NOT NULL DELETE FROM [idx_attr_comp].[pc_doc_compositeuniquedoc]";
        await cleanCmd.ExecuteNonQueryAsync();

        var doc = new CompositeUniqueDoc { Id = Guid.NewGuid(), FirstName = "John", LastName = "Doe" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // Verify the composite unique index exists with explicit name
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT is_unique FROM sys.indexes
            WHERE name = 'ux_fullname'
              AND object_id = OBJECT_ID('[idx_attr_comp].[pc_doc_compositeuniquedoc]')
            """;
        var isUnique = await cmd.ExecuteScalarAsync();
        isUnique.ShouldNotBeNull();
        ((bool)isUnique).ShouldBeTrue();

        // Second insert with same name should fail
        await using var session2 = theStore.LightweightSession();
        var doc2 = new CompositeUniqueDoc { Id = Guid.NewGuid(), FirstName = "John", LastName = "Doe" };
        session2.Store(doc2);

        await Should.ThrowAsync<Exception>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task attribute_index_with_casing()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_attr_case";
        });

        var doc = new CasingAttributeDoc
        {
            Id = Guid.NewGuid(),
            UserName = "JohnDoe",
            Email = $"Test-{Guid.NewGuid()}@Example.COM"
        };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();

        // Verify lower-cased index on UserName
        await using var cmd1 = conn.CreateCommand();
        cmd1.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE name = 'ix_pc_doc_casingattributedoc_username_lower'
              AND object_id = OBJECT_ID('[idx_attr_case].[pc_doc_casingattributedoc]')
            """;
        var count1 = (int)(await cmd1.ExecuteScalarAsync())!;
        count1.ShouldBe(1);

        // Verify upper-cased unique index on Email
        await using var cmd2 = conn.CreateCommand();
        cmd2.CommandText = """
            SELECT is_unique FROM sys.indexes
            WHERE name = 'ux_pc_doc_casingattributedoc_email_upper'
              AND object_id = OBJECT_ID('[idx_attr_case].[pc_doc_casingattributedoc]')
            """;
        var isUnique = await cmd2.ExecuteScalarAsync();
        isUnique.ShouldNotBeNull();
        ((bool)isUnique).ShouldBeTrue();

        // Verify the lower-cased computed column value
        await using var cmd3 = conn.CreateCommand();
        cmd3.CommandText = """
            SELECT [cc_username_lower] FROM [idx_attr_case].[pc_doc_casingattributedoc]
            WHERE id = @id
            """;
        cmd3.Parameters.AddWithValue("@id", doc.Id);
        var storedValue = (string?)(await cmd3.ExecuteScalarAsync());
        storedValue.ShouldBe("johndoe");
    }

    [Fact]
    public async Task attribute_index_with_custom_sql_type()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "idx_attr_sqltype";
        });

        var doc = new CustomSqlTypeAttributeDoc
        {
            Id = Guid.NewGuid(),
            Score = 42,
            LongDescription = "A long description value"
        };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes
            WHERE object_id = OBJECT_ID('[idx_attr_sqltype].[pc_doc_customsqltypeattributedoc]')
              AND name IN ('ix_pc_doc_customsqltypeattributedoc_score', 'ix_pc_doc_customsqltypeattributedoc_longdescription')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(2);
    }

    #endregion

    #region Unit Tests for DDL Generation

    [Fact]
    public void document_index_ddl_with_lower_casing()
    {
        var index = new DocumentIndex(["$.name"]) { Casing = IndexCasing.Lower };
        var mapping = new TestDocumentMapping("test_schema", "my_table");

        var statements = index.ToDdlStatements(mapping);

        // Should use LOWER() wrapping
        statements[0].ShouldContain("LOWER(CAST(JSON_VALUE(data, '$.name')");
        // Column name should include _lower suffix
        statements[0].ShouldContain("cc_name_lower");
    }

    [Fact]
    public void document_index_ddl_with_upper_casing()
    {
        var index = new DocumentIndex(["$.email"]) { Casing = IndexCasing.Upper };
        var mapping = new TestDocumentMapping("test_schema", "my_table");

        var statements = index.ToDdlStatements(mapping);

        // Should use UPPER() wrapping
        statements[0].ShouldContain("UPPER(CAST(JSON_VALUE(data, '$.email')");
        // Column name should include _upper suffix
        statements[0].ShouldContain("cc_email_upper");
    }

    [Fact]
    public void document_index_ddl_default_casing_has_no_wrapper()
    {
        var index = new DocumentIndex(["$.name"]);
        var mapping = new TestDocumentMapping("test_schema", "my_table");

        var statements = index.ToDdlStatements(mapping);

        // Should NOT contain UPPER or LOWER
        statements[0].ShouldNotContain("UPPER");
        statements[0].ShouldNotContain("LOWER");
        // Should contain plain CAST
        statements[0].ShouldContain("CAST(JSON_VALUE(data, '$.name')");
    }

    [Fact]
    public void column_name_includes_casing_suffix()
    {
        DocumentIndex.ColumnNameForPath("$.name", IndexCasing.Lower).ShouldBe("cc_name_lower");
        DocumentIndex.ColumnNameForPath("$.name", IndexCasing.Upper).ShouldBe("cc_name_upper");
        DocumentIndex.ColumnNameForPath("$.name", IndexCasing.Default).ShouldBe("cc_name");
        DocumentIndex.ColumnNameForPath("$.name").ShouldBe("cc_name");
    }

    #endregion
}

/// <summary>
///     Lightweight stub for unit-testing DocumentIndex DDL generation without a real database.
/// </summary>
internal class TestDocumentMapping : DocumentMapping
{
    public TestDocumentMapping(string schema, string table)
        : base(typeof(IndexedProduct), new StoreOptions { DatabaseSchemaName = schema })
    {
    }
}
