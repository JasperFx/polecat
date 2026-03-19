using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

// Hierarchy types
public abstract class User
{
    public Guid Id { get; set; }
    public string UserName { get; set; } = string.Empty;
}

public class AdminUser : User
{
    public string Region { get; set; } = string.Empty;
}

public class SuperUser : User
{
    public string Role { get; set; } = string.Empty;
}

[Collection("integration")]
public class document_hierarchy_tests : IntegrationContext
{
    public document_hierarchy_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task sanity_check_store_without_hierarchy()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_sanity";
        });

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "sanity", Region = "US" };
        theSession.Store(admin);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<AdminUser>(admin.Id);
        loaded.ShouldNotBeNull();
        loaded.UserName.ShouldBe("sanity");
    }

    [Fact]
    public async Task store_and_load_subclass_as_base()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_base";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "admin1", Region = "US" };
        theSession.Store(admin);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(admin.Id);

        loaded.ShouldNotBeNull();
        loaded.ShouldBeOfType<AdminUser>();
        ((AdminUser)loaded).Region.ShouldBe("US");
    }

    [Fact]
    public async Task store_and_load_subclass_directly()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_direct";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        var super = new SuperUser { Id = Guid.NewGuid(), UserName = "super1", Role = "Lead" };
        theSession.Store(super);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(super.Id);

        loaded.ShouldNotBeNull();
        loaded.ShouldBeOfType<SuperUser>();
        ((SuperUser)loaded).Role.ShouldBe("Lead");
    }

    [Fact]
    public async Task load_many_returns_mixed_types()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_many";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "admin2", Region = "EU" };
        var super = new SuperUser { Id = Guid.NewGuid(), UserName = "super2", Role = "Admin" };
        theSession.Store(admin);
        theSession.Store(super);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<User>(new[] { admin.Id, super.Id });

        loaded.Count.ShouldBe(2);
        loaded.ShouldContain(u => u is AdminUser);
        loaded.ShouldContain(u => u is SuperUser);
    }

    [Fact]
    public async Task query_all_returns_mixed_types()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_query_all";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        // Clean up leftover data from previous runs
        await using (var conn = await OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "IF OBJECT_ID('[hierarchy_query_all].[pc_doc_user]', 'U') IS NOT NULL DELETE FROM [hierarchy_query_all].[pc_doc_user]";
            await cmd.ExecuteNonQueryAsync();
        }

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "admin3", Region = "APAC" };
        var super1 = new SuperUser { Id = Guid.NewGuid(), UserName = "super3", Role = "Dev" };
        theSession.Store(admin);
        theSession.Store(super1);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var all = await query.Query<User>().ToListAsync();

        all.Count.ShouldBe(2);
        all.ShouldContain(u => u is AdminUser);
        all.ShouldContain(u => u is SuperUser);
    }

    [Fact]
    public async Task query_specific_subclass()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_query_sub";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        // Clean up leftover data from previous runs
        await using (var conn = await OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "IF OBJECT_ID('[hierarchy_query_sub].[pc_doc_user]', 'U') IS NOT NULL DELETE FROM [hierarchy_query_sub].[pc_doc_user]";
            await cmd.ExecuteNonQueryAsync();
        }

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "admin4", Region = "SA" };
        var super = new SuperUser { Id = Guid.NewGuid(), UserName = "super4", Role = "QA" };
        theSession.Store(admin);
        theSession.Store(super);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var admins = await query.Query<AdminUser>().ToListAsync();

        admins.Count.ShouldBe(1);
        admins[0].UserName.ShouldBe("admin4");
        admins[0].Region.ShouldBe("SA");
    }

    [Fact]
    public async Task upsert_subclass_preserves_doc_type()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_upsert";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "admin5", Region = "NA" };
        theSession.Store(admin);
        await theSession.SaveChangesAsync();

        // Update via upsert
        await using var session2 = theStore.LightweightSession();
        admin.Region = "EMEA";
        session2.Store(admin);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(admin.Id);

        loaded.ShouldBeOfType<AdminUser>();
        ((AdminUser)loaded).Region.ShouldBe("EMEA");
    }

    [Fact]
    public async Task custom_alias()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_alias";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>("admin")
                .AddSubClass<SuperUser>("super");
        });

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "admin6", Region = "UK" };
        theSession.Store(admin);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(admin.Id);

        loaded.ShouldNotBeNull();
        loaded.ShouldBeOfType<AdminUser>();
    }

    [Fact]
    public async Task add_sub_class_hierarchy_auto_discovery()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_auto";
            opts.Schema.For<User>()
                .AddSubClassHierarchy();
        });

        // Clean up leftover data from previous runs
        await using (var conn = await OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "IF OBJECT_ID('[hierarchy_auto].[pc_doc_user]', 'U') IS NOT NULL DELETE FROM [hierarchy_auto].[pc_doc_user]";
            await cmd.ExecuteNonQueryAsync();
        }

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "admin7", Region = "DE" };
        var super = new SuperUser { Id = Guid.NewGuid(), UserName = "super7", Role = "Mgr" };
        theSession.Store(admin);
        theSession.Store(super);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var all = await query.Query<User>().ToListAsync();

        all.Count.ShouldBe(2);
        all.ShouldContain(u => u is AdminUser);
        all.ShouldContain(u => u is SuperUser && u.Id == super.Id);
    }

    [Fact]
    public async Task linq_where_on_base_returns_correct_subclass()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_where";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "findme", Region = "FR" };
        var super = new SuperUser { Id = Guid.NewGuid(), UserName = "other", Role = "PM" };
        theSession.Store(admin);
        theSession.Store(super);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<User>()
            .Where(x => x.UserName == "findme")
            .FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result.ShouldBeOfType<AdminUser>();
    }

    [Fact]
    public async Task delete_subclass_document()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "hierarchy_delete";
            opts.Schema.For<User>()
                .AddSubClass<AdminUser>()
                .AddSubClass<SuperUser>();
        });

        var admin = new AdminUser { Id = Guid.NewGuid(), UserName = "deleteme", Region = "JP" };
        theSession.Store(admin);
        await theSession.SaveChangesAsync();

        // Verify the row exists before deleting
        await using var midQuery = theStore.QuerySession();
        var mid = await midQuery.LoadAsync<User>(admin.Id);
        mid.ShouldNotBeNull("Document should exist after Store");

        await using var session2 = theStore.LightweightSession();
        session2.Delete<User>(admin.Id);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(admin.Id);
        loaded.ShouldBeNull();
    }
}
