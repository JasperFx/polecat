using JasperFx;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.BulkInsert;

/// <summary>
///     Coverage for <see cref="AdvancedOperations.BulkInsertWithVersionAsync{T}"/>
///     — closes polecat#48 (the OverwriteIfVersionMatches behavior promised by
///     <see cref="BulkInsertMode.OverwriteIfVersionMatches"/> after the
///     weasel#264 enum promotion).
/// </summary>
[Collection("integration")]
public class bulk_insert_with_version_check : IntegrationContext
{
    public bulk_insert_with_version_check(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "bulk_insert_version"; });
    }

    [Fact]
    public async Task missing_id_is_inserted()
    {
        // When no row exists for the incoming id, the MERGE falls through to
        // the WHEN NOT MATCHED branch and inserts. The expected_version is
        // irrelevant on an insert and the row lands at version=1.
        var doc = new User
        {
            Id = Guid.NewGuid(),
            FirstName = "Fresh",
            LastName = "Insert",
            Age = 30
        };

        await theStore.Advanced.BulkInsertWithVersionAsync(new[] { (doc, 999L) });

        await using var session = theStore.QuerySession();
        var loaded = await session.LoadAsync<User>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Fresh");
    }

    [Fact]
    public async Task matching_version_updates_and_bumps_version()
    {
        var id = Guid.NewGuid();
        var original = new User { Id = id, FirstName = "Original", LastName = "Doe", Age = 25 };
        await theStore.Advanced.BulkInsertAsync(new[] { original });

        // After the initial bulk insert the row is at version 1.
        var updated = new User { Id = id, FirstName = "Updated", LastName = "Doe", Age = 99 };
        await theStore.Advanced.BulkInsertWithVersionAsync(new[] { (updated, 1L) });

        await using var session = theStore.QuerySession();
        var loaded = await session.LoadAsync<User>(id);
        loaded!.FirstName.ShouldBe("Updated");
        loaded.Age.ShouldBe(99);
    }

    [Fact]
    public async Task mismatched_version_throws_concurrency_exception()
    {
        var id = Guid.NewGuid();
        var original = new User { Id = id, FirstName = "Original", LastName = "Doe", Age = 25 };
        await theStore.Advanced.BulkInsertAsync(new[] { original });

        // Row is at version 1; pass an expected version of 999 — the
        // WHEN MATCHED AND ... predicate will fail and the row will be
        // neither updated nor inserted. The OUTPUT clause won't emit it,
        // so the post-batch check throws ConcurrencyException.
        var stale = new User { Id = id, FirstName = "Stale", LastName = "Doe", Age = 99 };

        var ex = await Should.ThrowAsync<ConcurrencyException>(async () =>
            await theStore.Advanced.BulkInsertWithVersionAsync(new[] { (stale, 999L) }));

        ex.Id.ShouldBe(id);

        // The original row should be untouched.
        await using var session = theStore.QuerySession();
        var loaded = await session.LoadAsync<User>(id);
        loaded!.FirstName.ShouldBe("Original");
        loaded.Age.ShouldBe(25);
    }

    [Fact]
    public async Task mixed_batch_partial_success_throws_on_first_mismatch()
    {
        // One existing row at version 1, one missing id, one existing row
        // we'll pass a wrong expected version for. The first two would
        // succeed in isolation; the third is the mismatch that throws.
        // Important: the throw happens *after* the SQL has run, so
        // matched-and-updated rows in the batch are committed (this is
        // how SQL Server MERGE batches behave) — the throw is a
        // best-effort signal, not a transactional rollback.
        var matchedId = Guid.NewGuid();
        var matchedOriginal = new User { Id = matchedId, FirstName = "Matched", LastName = "Initial", Age = 20 };
        await theStore.Advanced.BulkInsertAsync(new[] { matchedOriginal });

        var mismatchedId = Guid.NewGuid();
        var mismatchedOriginal = new User { Id = mismatchedId, FirstName = "Mismatched", LastName = "Initial", Age = 21 };
        await theStore.Advanced.BulkInsertAsync(new[] { mismatchedOriginal });

        var newId = Guid.NewGuid();

        var batch = new[]
        {
            (new User { Id = matchedId, FirstName = "Matched", LastName = "Updated", Age = 22 }, 1L),
            (new User { Id = newId, FirstName = "Fresh", LastName = "Insert", Age = 23 }, 0L),
            (new User { Id = mismatchedId, FirstName = "Mismatched", LastName = "Updated", Age = 24 }, 999L)
        };

        await Should.ThrowAsync<ConcurrencyException>(async () =>
            await theStore.Advanced.BulkInsertWithVersionAsync(batch));
    }

    [Fact]
    public async Task versionless_overload_with_version_mode_throws_helpful_invalidoperation()
    {
        // BulkInsertAsync (the versionless surface) cannot honor
        // OverwriteIfVersionMatches because it has no per-row version input.
        // The previous behavior was NotSupportedException with a pointer to
        // polecat#48; with #48 implemented, the error now points callers at
        // the new sibling method.
        var doc = new User { Id = Guid.NewGuid(), FirstName = "x", LastName = "y", Age = 1 };

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await theStore.Advanced.BulkInsertAsync(new[] { doc }, BulkInsertMode.OverwriteIfVersionMatches));

        ex.Message.ShouldContain("BulkInsertWithVersionAsync");
    }

    [Fact]
    public async Task empty_collection_does_nothing()
    {
        await theStore.Advanced.BulkInsertWithVersionAsync(Array.Empty<(User, long)>());
    }
}
