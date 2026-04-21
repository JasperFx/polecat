using Microsoft.EntityFrameworkCore;
using Weasel.EntityFrameworkCore;
using Weasel.SqlServer.Tables;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     Entities and DbContext for testing schema handling with EF Core integration.
///     Ported from Marten issue https://github.com/JasperFx/marten/issues/4175
/// </summary>
public class EntityType
{
    public int Id { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

public class Entity
{
    public Guid Id { get; set; }
    public int EntityTypeId { get; set; }
    public bool Featured { get; set; }
    public string InternalName { get; set; } = string.Empty;

    public EntityType EntityType { get; set; } = null!;
}

/// <summary>
///     DbContext that places entities in an explicit "test_ef_schema" schema,
///     separate from the Polecat document store schema.
/// </summary>
public class SeparateSchemaDbContext : DbContext
{
    public const string EfSchema = "test_ef_schema";

    public SeparateSchemaDbContext(DbContextOptions<SeparateSchemaDbContext> options) : base(options)
    {
    }

    public DbSet<Entity> Entities => Set<Entity>();
    public DbSet<EntityType> EntityTypes => Set<EntityType>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(EfSchema);

        modelBuilder.Entity<EntityType>(entity =>
        {
            entity.ToTable("entity_type");
            entity.HasKey(e => e.Id);
        });

        modelBuilder.Entity<Entity>(entity =>
        {
            entity.ToTable("entity");
            entity.HasKey(e => e.Id);
            entity.HasOne(e => e.EntityType)
                .WithMany()
                .HasForeignKey(e => e.EntityTypeId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}

public class EfCoreSchemaTests
{
    [Fact]
    public void should_respect_ef_core_explicit_schema_and_not_move_tables_to_polecat_schema()
    {
        // Ported from Marten issue #4175: When EF Core entities have an explicit schema
        // configured, AddEntityTablesFromDbContext should NOT move those tables.
        const string polecatSchema = "test_polecat_schema";

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = polecatSchema;

            opts.AddEntityTablesFromDbContext<SeparateSchemaDbContext>();
        });

        var extendedObjects = store.Options.ExtendedSchemaObjects;

        var entityTable = extendedObjects.OfType<Table>()
            .FirstOrDefault(t => t.Identifier.Name == "entity");
        var entityTypeTable = extendedObjects.OfType<Table>()
            .FirstOrDefault(t => t.Identifier.Name == "entity_type");

        entityTable.ShouldNotBeNull();
        entityTypeTable.ShouldNotBeNull();

        // These tables should remain in the EF Core schema, NOT Polecat's schema
        entityTable.Identifier.Schema.ShouldBe(SeparateSchemaDbContext.EfSchema,
            "Entity table should stay in the EF Core schema, not be moved to Polecat's schema");
        entityTypeTable.Identifier.Schema.ShouldBe(SeparateSchemaDbContext.EfSchema,
            "EntityType table should stay in the EF Core schema, not be moved to Polecat's schema");
    }

    [Fact]
    public void should_use_default_schema_for_tables_without_explicit_schema()
    {
        // When EF Core entities do NOT have an explicit schema,
        // they should use the SQL Server default schema ("dbo").
        const string polecatSchema = "test_polecat_schema";

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = polecatSchema;

            // TestDbContext does not set an explicit schema
            opts.AddEntityTablesFromDbContext<TestDbContext>();
        });

        var extendedObjects = store.Options.ExtendedSchemaObjects;

        var orderSummariesTable = extendedObjects.OfType<Table>()
            .FirstOrDefault(t => t.Identifier.Name == "ef_order_summaries");

        orderSummariesTable.ShouldNotBeNull();
        orderSummariesTable.Identifier.Schema.ShouldBe("dbo",
            "Tables without explicit schema should use the default SQL Server schema");
    }

    [Fact]
    public void should_register_all_entity_tables_from_db_context()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AddEntityTablesFromDbContext<SeparateSchemaDbContext>();
        });

        var tables = store.Options.ExtendedSchemaObjects.OfType<Table>().ToList();

        tables.Count.ShouldBe(2);
        tables.ShouldContain(t => t.Identifier.Name == "entity");
        tables.ShouldContain(t => t.Identifier.Name == "entity_type");
    }

    [Fact]
    public void should_map_columns_from_entity_types()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AddEntityTablesFromDbContext<SeparateSchemaDbContext>();
        });

        var entityTable = store.Options.ExtendedSchemaObjects.OfType<Table>()
            .First(t => t.Identifier.Name == "entity");

        // Verify expected number of columns were mapped (Id, EntityTypeId, Featured, InternalName)
        entityTable.Columns.Count.ShouldBe(4);

        // Weasel normalizes column names to lowercase
        var columnNames = entityTable.Columns.Select(c => c.Name).ToList();
        columnNames.ShouldContain("id");
        columnNames.ShouldContain("entitytypeid");
        columnNames.ShouldContain("featured");
        columnNames.ShouldContain("internalname");
    }

    [Fact]
    public void should_map_foreign_keys_from_entity_types()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AddEntityTablesFromDbContext<SeparateSchemaDbContext>();
        });

        var entityTable = store.Options.ExtendedSchemaObjects.OfType<Table>()
            .First(t => t.Identifier.Name == "entity");

        // Entity has a FK to EntityType
        entityTable.ForeignKeys.Count.ShouldBeGreaterThan(0);
        entityTable.ForeignKeys.ShouldContain(fk =>
            fk.LinkedTable!.Name == "entity_type");
    }

    [Fact]
    public void should_return_entity_types_in_fk_dependency_order()
    {
        // Issue https://github.com/JasperFx/marten/issues/4180:
        // GetEntityTypesForMigration should return entity types sorted
        // so that referenced tables come before referencing tables.
        var builder = new DbContextOptionsBuilder<SeparateSchemaDbContext>();
        builder.UseSqlServer("Server=localhost");

        using var dbContext = new SeparateSchemaDbContext(builder.Options);

        var entityTypes = DbContextExtensions.GetEntityTypesForMigration(dbContext);
        var names = entityTypes.Select(e => e.GetTableName()).ToList();

        // entity_type must come before entity because entity has a FK to entity_type
        var entityTypeIndex = names.IndexOf("entity_type");
        var entityIndex = names.IndexOf("entity");

        entityTypeIndex.ShouldBeGreaterThanOrEqualTo(0);
        entityIndex.ShouldBeGreaterThanOrEqualTo(0);
        entityTypeIndex.ShouldBeLessThan(entityIndex,
            "entity_type should come before entity due to FK dependency (Marten issue #4180)");
    }

    [Fact]
    public void should_register_tables_in_fk_dependency_order()
    {
        // Verify the tables are registered in dependency order via AddEntityTablesFromDbContext
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AddEntityTablesFromDbContext<SeparateSchemaDbContext>();
        });

        var tables = store.Options.ExtendedSchemaObjects.OfType<Table>().ToList();
        var tableNames = tables.Select(t => t.Identifier.Name).ToList();

        var entityTypeIndex = tableNames.IndexOf("entity_type");
        var entityIndex = tableNames.IndexOf("entity");

        entityTypeIndex.ShouldBeGreaterThanOrEqualTo(0);
        entityIndex.ShouldBeGreaterThanOrEqualTo(0);
        entityTypeIndex.ShouldBeLessThan(entityIndex,
            "entity_type table should be registered before entity table due to FK dependency");
    }

    [Fact]
    public void should_have_correct_fk_schema_with_explicit_schema()
    {
        // Related to Marten issue #4192: FK LinkedTable references should use the
        // correct schema. In Polecat's case (SQL Server), tables with an explicit
        // schema should have FKs pointing to the same explicit schema.
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AddEntityTablesFromDbContext<SeparateSchemaDbContext>();
        });

        var entityTable = store.Options.ExtendedSchemaObjects.OfType<Table>()
            .First(t => t.Identifier.Name == "entity");

        var entityFk = entityTable.ForeignKeys.FirstOrDefault();
        entityFk.ShouldNotBeNull("Entity should have a FK to EntityType");
        entityFk.LinkedTable!.Schema.ShouldBe(SeparateSchemaDbContext.EfSchema,
            "FK LinkedTable should reference the correct explicit schema");
    }
}
