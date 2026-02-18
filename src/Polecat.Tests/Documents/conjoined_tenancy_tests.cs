using Polecat.Tests.Harness;

namespace Polecat.Tests.Documents;

[Collection("integration")]
public class conjoined_tenancy_tests : IntegrationContext
{
    public conjoined_tenancy_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateConjoinedStore()
    {
        var schemaName = "tenancy_docs_" + Guid.NewGuid().ToString("N")[..8];
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schemaName;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });
        return theStore;
    }

    [Fact]
    public async Task store_and_load_isolated_by_tenant()
    {
        var store = await CreateConjoinedStore();
        var id = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Store(new User { Id = id, FirstName = "Red", LastName = "User", Age = 30 });
        await redSession.SaveChangesAsync();

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var loaded = await blueQuery.LoadAsync<User>(id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task store_same_id_different_tenants()
    {
        var store = await CreateConjoinedStore();
        var id = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Store(new User { Id = id, FirstName = "RedFirst", LastName = "RedLast", Age = 10 });
        await redSession.SaveChangesAsync();

        await using var blueSession = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
        blueSession.Store(new User { Id = id, FirstName = "BlueFirst", LastName = "BlueLast", Age = 20 });
        await blueSession.SaveChangesAsync();

        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var redUser = await redQuery.LoadAsync<User>(id);
        redUser.ShouldNotBeNull();
        redUser.FirstName.ShouldBe("RedFirst");

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blueUser = await blueQuery.LoadAsync<User>(id);
        blueUser.ShouldNotBeNull();
        blueUser.FirstName.ShouldBe("BlueFirst");
    }

    [Fact]
    public async Task delete_is_tenant_scoped()
    {
        var store = await CreateConjoinedStore();
        var id = Guid.NewGuid();

        // Store same doc in both tenants
        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Store(new User { Id = id, FirstName = "Red", LastName = "User", Age = 1 });
        await redSession.SaveChangesAsync();

        await using var blueSession = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
        blueSession.Store(new User { Id = id, FirstName = "Blue", LastName = "User", Age = 2 });
        await blueSession.SaveChangesAsync();

        // Delete as Red
        await using var redDelete = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redDelete.Delete<User>(id);
        await redDelete.SaveChangesAsync();

        // Blue's doc should still exist
        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blueUser = await blueQuery.LoadAsync<User>(id);
        blueUser.ShouldNotBeNull();
        blueUser.FirstName.ShouldBe("Blue");

        // Red's doc should be gone
        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var redUser = await redQuery.LoadAsync<User>(id);
        redUser.ShouldBeNull();
    }

    [Fact]
    public async Task insert_is_tenant_scoped()
    {
        var store = await CreateConjoinedStore();
        var id = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Insert(new User { Id = id, FirstName = "Red", LastName = "Insert", Age = 1 });
        await redSession.SaveChangesAsync();

        // Same ID in different tenant should not conflict
        await using var blueSession = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
        blueSession.Insert(new User { Id = id, FirstName = "Blue", LastName = "Insert", Age = 2 });
        await blueSession.SaveChangesAsync();

        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var red = await redQuery.LoadAsync<User>(id);
        red.ShouldNotBeNull();
        red.FirstName.ShouldBe("Red");

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blue = await blueQuery.LoadAsync<User>(id);
        blue.ShouldNotBeNull();
        blue.FirstName.ShouldBe("Blue");
    }

    [Fact]
    public async Task update_is_tenant_scoped()
    {
        var store = await CreateConjoinedStore();
        var id = Guid.NewGuid();

        // Insert in both tenants
        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Insert(new User { Id = id, FirstName = "Original", LastName = "Red", Age = 1 });
        await redSession.SaveChangesAsync();

        await using var blueSession = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
        blueSession.Insert(new User { Id = id, FirstName = "Original", LastName = "Blue", Age = 2 });
        await blueSession.SaveChangesAsync();

        // Update only Red
        await using var redUpdate = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redUpdate.Update(new User { Id = id, FirstName = "Updated", LastName = "Red", Age = 99 });
        await redUpdate.SaveChangesAsync();

        // Blue should be unchanged
        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blue = await blueQuery.LoadAsync<User>(id);
        blue.ShouldNotBeNull();
        blue.FirstName.ShouldBe("Original");
        blue.LastName.ShouldBe("Blue");

        // Red should be updated
        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var red = await redQuery.LoadAsync<User>(id);
        red.ShouldNotBeNull();
        red.FirstName.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_many_is_tenant_scoped()
    {
        var store = await CreateConjoinedStore();
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Store(new User { Id = id1, FirstName = "Red1", LastName = "User", Age = 1 });
        redSession.Store(new User { Id = id2, FirstName = "Red2", LastName = "User", Age = 2 });
        await redSession.SaveChangesAsync();

        await using var blueSession = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
        blueSession.Store(new User { Id = id1, FirstName = "Blue1", LastName = "User", Age = 3 });
        await blueSession.SaveChangesAsync();

        // Red should see both
        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var redDocs = await redQuery.LoadManyAsync<User>(new[] { id1, id2 });
        redDocs.Count.ShouldBe(2);

        // Blue should only see one
        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blueDocs = await blueQuery.LoadManyAsync<User>(new[] { id1, id2 });
        blueDocs.Count.ShouldBe(1);
        blueDocs[0].FirstName.ShouldBe("Blue1");
    }
}
