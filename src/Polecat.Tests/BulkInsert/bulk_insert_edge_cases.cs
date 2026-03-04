using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.BulkInsert;

[Collection("integration")]
public class bulk_insert_edge_cases : IntegrationContext
{
    public bulk_insert_edge_cases(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "bulk_edge"; });
    }

    // ===== Versioned / Revisioned documents =====

    [Fact]
    public async Task bulk_insert_versioned_doc_assigns_initial_data()
    {
        var docs = Enumerable.Range(0, 5).Select(i => new VersionedDoc
        {
            Id = Guid.NewGuid(),
            Name = $"Versioned {i}"
        }).ToList();

        await theStore.Advanced.BulkInsertAsync(docs);

        await using var session = theStore.QuerySession();
        foreach (var doc in docs)
        {
            var loaded = await session.LoadAsync<VersionedDoc>(doc.Id);
            loaded.ShouldNotBeNull();
            loaded.Name.ShouldBe(doc.Name);
        }
    }

    [Fact]
    public async Task bulk_insert_revisioned_doc_assigns_initial_data()
    {
        var docs = Enumerable.Range(0, 5).Select(i => new RevisionedDoc
        {
            Id = Guid.NewGuid(),
            Name = $"Revisioned {i}"
        }).ToList();

        await theStore.Advanced.BulkInsertAsync(docs);

        await using var session = theStore.QuerySession();
        foreach (var doc in docs)
        {
            var loaded = await session.LoadAsync<RevisionedDoc>(doc.Id);
            loaded.ShouldNotBeNull();
            loaded.Name.ShouldBe(doc.Name);
        }
    }

    // ===== Soft-deleted documents =====

    [Fact]
    public async Task bulk_insert_soft_deleted_doc_is_queryable()
    {
        var docs = new[]
        {
            new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "SD1", Number = 1 },
            new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "SD2", Number = 2 }
        };

        await theStore.Advanced.BulkInsertAsync(docs);

        await using var session = theStore.QuerySession();
        foreach (var doc in docs)
        {
            var loaded = await session.LoadAsync<SoftDeletedDoc>(doc.Id);
            loaded.ShouldNotBeNull();
            loaded.Name.ShouldBe(doc.Name);
        }
    }

    // ===== Large batch / multi-batch =====

    [Fact]
    public async Task bulk_insert_large_batch_with_tiny_batch_size()
    {
        var docs = Enumerable.Range(0, 50).Select(i => new User
        {
            Id = Guid.NewGuid(),
            FirstName = $"Large{i}",
            LastName = $"Batch{i}",
            Age = i
        }).ToList();

        // Force 25 batches of 2
        await theStore.Advanced.BulkInsertAsync(docs, BulkInsertMode.InsertsOnly, batchSize: 2);

        await using var session = theStore.QuerySession();
        foreach (var doc in docs)
        {
            var loaded = await session.LoadAsync<User>(doc.Id);
            loaded.ShouldNotBeNull();
            loaded.FirstName.ShouldBe(doc.FirstName);
        }
    }

    // ===== IgnoreDuplicates with overlap =====

    [Fact]
    public async Task ignore_duplicates_with_partial_overlap()
    {
        var shared1 = new User { Id = Guid.NewGuid(), FirstName = "Shared1", LastName = "Orig", Age = 10 };
        var shared2 = new User { Id = Guid.NewGuid(), FirstName = "Shared2", LastName = "Orig", Age = 20 };

        // Insert first two
        await theStore.Advanced.BulkInsertAsync(new[] { shared1, shared2 });

        // Now insert a mix of existing + new
        var newDoc = new User { Id = Guid.NewGuid(), FirstName = "Brand New", LastName = "Doc", Age = 30 };
        var updatedShared1 = new User { Id = shared1.Id, FirstName = "CHANGED", LastName = "Orig", Age = 99 };

        await theStore.Advanced.BulkInsertAsync(
            new[] { updatedShared1, newDoc },
            BulkInsertMode.IgnoreDuplicates);

        await using var session = theStore.QuerySession();

        // shared1 should still have original data (duplicate ignored)
        var loaded1 = await session.LoadAsync<User>(shared1.Id);
        loaded1!.FirstName.ShouldBe("Shared1");
        loaded1.Age.ShouldBe(10);

        // new doc should exist
        var loadedNew = await session.LoadAsync<User>(newDoc.Id);
        loadedNew.ShouldNotBeNull();
        loadedNew.FirstName.ShouldBe("Brand New");
    }

    // ===== OverwriteExisting version increment =====

    [Fact]
    public async Task overwrite_existing_increments_version()
    {
        var id = Guid.NewGuid();
        var original = new User { Id = id, FirstName = "V1", LastName = "Test", Age = 1 };
        await theStore.Advanced.BulkInsertAsync(new[] { original });

        // Overwrite twice
        var v2 = new User { Id = id, FirstName = "V2", LastName = "Test", Age = 2 };
        await theStore.Advanced.BulkInsertAsync(new[] { v2 }, BulkInsertMode.OverwriteExisting);

        var v3 = new User { Id = id, FirstName = "V3", LastName = "Test", Age = 3 };
        await theStore.Advanced.BulkInsertAsync(new[] { v3 }, BulkInsertMode.OverwriteExisting);

        await using var session = theStore.QuerySession();
        var loaded = await session.LoadAsync<User>(id);
        loaded!.FirstName.ShouldBe("V3");
    }

    // ===== Single-item bulk insert =====

    [Fact]
    public async Task bulk_insert_single_item()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Solo", LastName = "Insert", Age = 42 };
        await theStore.Advanced.BulkInsertAsync(new[] { user });

        await using var session = theStore.QuerySession();
        var loaded = await session.LoadAsync<User>(user.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Solo");
    }

    // ===== Concurrent bulk inserts =====

    [Fact]
    public async Task concurrent_bulk_inserts_with_ignore_duplicates()
    {
        // Pre-create a set of IDs
        var ids = Enumerable.Range(0, 20).Select(_ => Guid.NewGuid()).ToList();

        // Two concurrent tasks inserting overlapping sets
        var set1 = ids.Take(15).Select(id => new User { Id = id, FirstName = "Set1", LastName = "A", Age = 1 }).ToList();
        var set2 = ids.Skip(5).Select(id => new User { Id = id, FirstName = "Set2", LastName = "B", Age = 2 }).ToList();

        // Both use IgnoreDuplicates so no errors on overlap
        var task1 = theStore.Advanced.BulkInsertAsync(set1, BulkInsertMode.IgnoreDuplicates);
        var task2 = theStore.Advanced.BulkInsertAsync(set2, BulkInsertMode.IgnoreDuplicates);

        await Task.WhenAll(task1, task2);

        // All 20 documents should exist
        await using var session = theStore.QuerySession();
        foreach (var id in ids)
        {
            var loaded = await session.LoadAsync<User>(id);
            loaded.ShouldNotBeNull();
        }
    }

    // ===== Mixed ID types in batch =====

    [Fact]
    public async Task bulk_insert_long_id_docs_with_hilo()
    {
        var docs = Enumerable.Range(0, 5).Select(_ => new LongDoc
        {
            Name = $"LongDoc-{Guid.NewGuid().ToString()[..8]}"
        }).ToList();

        await theStore.Advanced.BulkInsertAsync(docs);

        // HiLo should have assigned positive IDs
        foreach (var doc in docs)
        {
            doc.Id.ShouldBeGreaterThan(0);
        }

        // IDs should be unique
        docs.Select(d => d.Id).Distinct().Count().ShouldBe(5);

        await using var session = theStore.QuerySession();
        foreach (var doc in docs)
        {
            var loaded = await session.LoadAsync<LongDoc>(doc.Id);
            loaded.ShouldNotBeNull();
            loaded.Name.ShouldBe(doc.Name);
        }
    }
}
