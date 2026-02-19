using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.BulkInsert;

[Collection("integration")]
public class bulk_insert_operations : IntegrationContext
{
    public bulk_insert_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "bulk_insert"; });
    }

    [Fact]
    public async Task can_bulk_insert_with_defaults()
    {
        var users = Enumerable.Range(0, 10).Select(i => new User
        {
            Id = Guid.NewGuid(),
            FirstName = $"First{i}",
            LastName = $"Last{i}",
            Age = 20 + i
        }).ToList();

        await theStore.Advanced.BulkInsertAsync(users);

        await using var session = theStore.QuerySession();
        foreach (var user in users)
        {
            var loaded = await session.LoadAsync<User>(user.Id);
            loaded.ShouldNotBeNull();
            loaded.FirstName.ShouldBe(user.FirstName);
        }
    }

    [Fact]
    public async Task can_bulk_insert_with_string_ids()
    {
        var suffix = Guid.NewGuid().ToString()[..8];
        var docs = Enumerable.Range(0, 5).Select(i => new StringDoc
        {
            Id = $"string-bulk-{suffix}-{i}",
            Name = $"Doc {i}"
        }).ToList();

        await theStore.Advanced.BulkInsertAsync(docs);

        await using var session = theStore.QuerySession();
        foreach (var doc in docs)
        {
            var loaded = await session.LoadAsync<StringDoc>(doc.Id);
            loaded.ShouldNotBeNull();
            loaded.Name.ShouldBe(doc.Name);
        }
    }

    [Fact]
    public async Task can_bulk_insert_with_hilo_int_ids()
    {
        var docs = Enumerable.Range(0, 5).Select(_ => new IntDoc
        {
            Name = $"IntDoc-{Guid.NewGuid().ToString()[..8]}"
        }).ToList();

        await theStore.Advanced.BulkInsertAsync(docs);

        // HiLo should have assigned IDs
        foreach (var doc in docs)
        {
            doc.Id.ShouldBeGreaterThan(0);
        }

        await using var session = theStore.QuerySession();
        foreach (var doc in docs)
        {
            var loaded = await session.LoadAsync<IntDoc>(doc.Id);
            loaded.ShouldNotBeNull();
            loaded.Name.ShouldBe(doc.Name);
        }
    }

    [Fact]
    public async Task inserts_only_mode_throws_on_duplicate()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Dup", LastName = "Test", Age = 30 };

        await theStore.Advanced.BulkInsertAsync(new[] { user });

        // Inserting again should throw
        await Should.ThrowAsync<SqlException>(async () =>
        {
            await theStore.Advanced.BulkInsertAsync(new[] { user }, BulkInsertMode.InsertsOnly);
        });
    }

    [Fact]
    public async Task ignore_duplicates_mode_skips_existing()
    {
        var id = Guid.NewGuid();
        var original = new User { Id = id, FirstName = "Original", LastName = "User", Age = 25 };
        await theStore.Advanced.BulkInsertAsync(new[] { original });

        var duplicate = new User { Id = id, FirstName = "Updated", LastName = "User", Age = 99 };
        await theStore.Advanced.BulkInsertAsync(new[] { duplicate }, BulkInsertMode.IgnoreDuplicates);

        // Should still have original data
        await using var session = theStore.QuerySession();
        var loaded = await session.LoadAsync<User>(id);
        loaded!.FirstName.ShouldBe("Original");
        loaded.Age.ShouldBe(25);
    }

    [Fact]
    public async Task overwrite_existing_mode_updates()
    {
        var id = Guid.NewGuid();
        var original = new User { Id = id, FirstName = "Original", LastName = "User", Age = 25 };
        await theStore.Advanced.BulkInsertAsync(new[] { original });

        var updated = new User { Id = id, FirstName = "Updated", LastName = "User", Age = 99 };
        await theStore.Advanced.BulkInsertAsync(new[] { updated }, BulkInsertMode.OverwriteExisting);

        await using var session = theStore.QuerySession();
        var loaded = await session.LoadAsync<User>(id);
        loaded!.FirstName.ShouldBe("Updated");
        loaded.Age.ShouldBe(99);
    }

    [Fact]
    public async Task bulk_insert_respects_batching()
    {
        // Insert more documents than the batch size to verify batching works
        var users = Enumerable.Range(0, 25).Select(i => new User
        {
            Id = Guid.NewGuid(),
            FirstName = $"Batch{i}",
            LastName = $"Test{i}",
            Age = i
        }).ToList();

        // Small batch size to force multiple batches
        await theStore.Advanced.BulkInsertAsync(users, BulkInsertMode.InsertsOnly, batchSize: 5);

        await using var session = theStore.QuerySession();
        foreach (var user in users)
        {
            var loaded = await session.LoadAsync<User>(user.Id);
            loaded.ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task bulk_insert_with_empty_collection_does_nothing()
    {
        // Should not throw
        await theStore.Advanced.BulkInsertAsync(Array.Empty<User>());
    }

    [Fact]
    public async Task bulk_insert_syncs_itenanted_metadata()
    {
        var docs = new[]
        {
            new TenantedBulkDoc { Id = Guid.NewGuid(), Name = "T1" },
            new TenantedBulkDoc { Id = Guid.NewGuid(), Name = "T2" }
        };

        await theStore.Advanced.BulkInsertAsync(docs);

        // ITenanted.TenantId should have been set to the default tenant
        foreach (var doc in docs)
        {
            doc.TenantId.ShouldBe(Tenancy.DefaultTenantId);
        }
    }

    [Fact]
    public async Task overwrite_existing_inserts_new_rows_too()
    {
        var existing = new User { Id = Guid.NewGuid(), FirstName = "Existing", LastName = "One", Age = 40 };
        await theStore.Advanced.BulkInsertAsync(new[] { existing });

        var newDoc = new User { Id = Guid.NewGuid(), FirstName = "New", LastName = "Doc", Age = 22 };
        var updatedExisting = new User
        {
            Id = existing.Id, FirstName = "Changed", LastName = "One", Age = 41
        };

        await theStore.Advanced.BulkInsertAsync(new[] { updatedExisting, newDoc },
            BulkInsertMode.OverwriteExisting);

        await using var session = theStore.QuerySession();
        var loadedExisting = await session.LoadAsync<User>(existing.Id);
        loadedExisting!.FirstName.ShouldBe("Changed");

        var loadedNew = await session.LoadAsync<User>(newDoc.Id);
        loadedNew.ShouldNotBeNull();
        loadedNew.FirstName.ShouldBe("New");
    }
}

public class TenantedBulkDoc : ITenanted
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
}
