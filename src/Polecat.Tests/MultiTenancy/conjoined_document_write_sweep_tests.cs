using JasperFx;
using Polecat.Linq;
using Polecat.Linq.SoftDeletes;
using Polecat.Attributes;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.MultiTenancy;

/// <summary>A conjoined document written every way there is.</summary>
public class PennedNote
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = "";
    public string Body { get; set; } = "";
    public int Revision { get; set; }
}

/// <summary>The same, soft-deleted, so the delete arms can tell soft from hard.</summary>
[SoftDeleted]
public class PennedSoftNote
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = "";
    public string Body { get; set; } = "";
}

/// <summary>
///     #681 — the write half of the conjoined-document sweep: <c>Patch</c>, soft delete,
///     <c>DeleteWhere</c>/<c>HardDeleteWhere</c>/<c>UndoDeleteWhere</c>, and bulk insert.
/// </summary>
/// <remarks>
///     Every arm uses <b>one id shared by two tenants</b> and asserts <b>both</b> directions, because a
///     set-based write is the shape where a missing tenant filter is most expensive and least visible:
///     "the row I meant to change did change" is true whether or not the other tenant's row changed
///     with it. So each test checks the target moved <em>and</em> that the neighbour did not.
/// </remarks>
public class conjoined_document_write_sweep_tests : OneOffConfigurationsContext
{
    private static readonly Guid SharedId = Guid.NewGuid();

    private void ConfigureConjoined()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<PennedNote>().MultiTenanted();
            opts.Schema.For<PennedSoftNote>().MultiTenanted();
        });
    }

    private async Task SeedAsync()
    {
        var token = TestContext.Current.CancellationToken;
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Store(new PennedNote { Id = SharedId, Owner = "Red", Body = "red body", Revision = 1 });
            red.Store(new PennedSoftNote { Id = SharedId, Owner = "Red", Body = "red body" });
            await red.SaveChangesAsync(token);
        }

        await using var blue = theStore.LightweightSession("Blue");
        blue.Store(new PennedNote { Id = SharedId, Owner = "Blue", Body = "blue body", Revision = 1 });
        blue.Store(new PennedSoftNote { Id = SharedId, Owner = "Blue", Body = "blue body" });
        await blue.SaveChangesAsync(token);
    }

    [Fact]
    public async Task patch_by_id_only_touches_the_tenants_own_row()
    {
        ConfigureConjoined();
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Patch<PennedNote>(SharedId).Set(x => x.Body, "patched");
            await red.SaveChangesAsync(token);
        }

        await using var query = theStore.QuerySession("Red");
        (await query.LoadAsync<PennedNote>(SharedId, token)).ShouldNotBeNull()!.Body.ShouldBe("patched");

        await using var blue = theStore.QuerySession("Blue");
        (await blue.LoadAsync<PennedNote>(SharedId, token)).ShouldNotBeNull()!.Body.ShouldBe("blue body");
    }

    [Fact]
    public async Task patch_by_filter_only_touches_the_tenants_own_rows()
    {
        ConfigureConjoined();
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        // The filter matches BOTH tenants' rows on its own terms. Only the session's tenant may move.
        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Patch<PennedNote>(x => x.Revision == 1).Set(x => x.Revision, 2);
            await red.SaveChangesAsync(token);
        }

        await using var redQuery = theStore.QuerySession("Red");
        (await redQuery.LoadAsync<PennedNote>(SharedId, token)).ShouldNotBeNull()!.Revision.ShouldBe(2);

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.LoadAsync<PennedNote>(SharedId, token)).ShouldNotBeNull()!.Revision.ShouldBe(1);
    }

    [Fact]
    public async Task soft_delete_by_id_only_deletes_the_tenants_own_row()
    {
        ConfigureConjoined();
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Delete<PennedSoftNote>(SharedId);
            await red.SaveChangesAsync(token);
        }

        await using var redQuery = theStore.QuerySession("Red");
        (await redQuery.LoadAsync<PennedSoftNote>(SharedId, token)).ShouldBeNull();
        // Soft, not hard: still there when asked for deleted rows.
        (await redQuery.Query<PennedSoftNote>().MaybeDeleted().CountAsync(token)).ShouldBe(1);

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.LoadAsync<PennedSoftNote>(SharedId, token)).ShouldNotBeNull()!.Owner.ShouldBe("Blue");
    }

    [Fact]
    public async Task delete_where_only_deletes_the_tenants_own_rows()
    {
        ConfigureConjoined();
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        // No Where of its own that could accidentally separate the tenants: this predicate matches both.
        await using (var red = theStore.LightweightSession("Red"))
        {
            red.DeleteWhere<PennedSoftNote>(x => x.Body != "");
            await red.SaveChangesAsync(token);
        }

        await using var redQuery = theStore.QuerySession("Red");
        (await redQuery.Query<PennedSoftNote>().CountAsync(token)).ShouldBe(0);

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.Query<PennedSoftNote>().CountAsync(token)).ShouldBe(1);
    }

    [Fact]
    public async Task undo_delete_where_only_restores_the_tenants_own_rows()
    {
        ConfigureConjoined();
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        // Soft-delete both tenants' rows, each through its own session, then undo one.
        foreach (var tenant in new[] { "Red", "Blue" })
        {
            await using var session = theStore.LightweightSession(tenant);
            session.Delete<PennedSoftNote>(SharedId);
            await session.SaveChangesAsync(token);
        }

        await using (var red = theStore.LightweightSession("Red"))
        {
            red.UndoDeleteWhere<PennedSoftNote>(x => x.Body != "");
            await red.SaveChangesAsync(token);
        }

        await using var redQuery = theStore.QuerySession("Red");
        (await redQuery.LoadAsync<PennedSoftNote>(SharedId, token)).ShouldNotBeNull()!.Owner.ShouldBe("Red");

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.LoadAsync<PennedSoftNote>(SharedId, token)).ShouldBeNull();
    }

    [Fact]
    public async Task hard_delete_where_only_removes_the_tenants_own_rows()
    {
        ConfigureConjoined();
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using (var red = theStore.LightweightSession("Red"))
        {
            red.HardDeleteWhere<PennedSoftNote>(x => x.Body != "");
            await red.SaveChangesAsync(token);
        }

        await using var redQuery = theStore.QuerySession("Red");
        // Gone for real — not even as a deleted row.
        (await redQuery.Query<PennedSoftNote>().MaybeDeleted().CountAsync(token)).ShouldBe(0);

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.Query<PennedSoftNote>().MaybeDeleted().CountAsync(token)).ShouldBe(1);
    }

    [Fact]
    public async Task bulk_insert_writes_under_the_tenant_it_is_given()
    {
        ConfigureConjoined();
        var token = TestContext.Current.CancellationToken;
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        // A NON-partitioned conjoined store, which is the arrangement this issue calls out: tenancy is
        // a column here, so nothing about the physical layout makes the tenant land correctly.
        await theStore.Advanced.BulkInsertAsync(
            [new PennedNote { Id = SharedId, Owner = "Red", Body = "bulk red", Revision = 1 }],
            BulkInsertMode.InsertsOnly, 200, "Red", token);

        await using (var redQuery = theStore.QuerySession("Red"))
        {
            (await redQuery.LoadAsync<PennedNote>(SharedId, token)).ShouldNotBeNull()!.Body.ShouldBe("bulk red");
        }

        await using (var blueQuery = theStore.QuerySession("Blue"))
        {
            (await blueQuery.LoadAsync<PennedNote>(SharedId, token)).ShouldBeNull();
        }
    }

    [Fact]
    public async Task bulk_insert_of_a_shared_id_into_a_second_tenant_is_not_a_duplicate()
    {
        ConfigureConjoined();
        var token = TestContext.Current.CancellationToken;
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await theStore.Advanced.BulkInsertAsync(
            [new PennedNote { Id = SharedId, Owner = "Red", Body = "bulk red", Revision = 1 }],
            BulkInsertMode.InsertsOnly, 200, "Red", token);

        // The same id under a DIFFERENT tenant is a different row, so InsertsOnly — which surfaces a
        // real duplicate — must accept it. This is the arm that would fail on a primary key that
        // forgot tenant_id.
        await theStore.Advanced.BulkInsertAsync(
            [new PennedNote { Id = SharedId, Owner = "Blue", Body = "bulk blue", Revision = 1 }],
            BulkInsertMode.InsertsOnly, 200, "Blue", token);

        await using (var redQuery = theStore.QuerySession("Red"))
        {
            (await redQuery.LoadAsync<PennedNote>(SharedId, token)).ShouldNotBeNull()!.Body.ShouldBe("bulk red");
        }

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.LoadAsync<PennedNote>(SharedId, token)).ShouldNotBeNull()!.Body.ShouldBe("bulk blue");
    }

    [Fact]
    public async Task ignore_duplicates_still_ignores_a_duplicate_within_the_same_tenant()
    {
        ConfigureConjoined();
        var token = TestContext.Current.CancellationToken;
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await theStore.Advanced.BulkInsertAsync(
            [new PennedNote { Id = SharedId, Owner = "Red", Body = "first", Revision = 1 }],
            BulkInsertMode.InsertsOnly, 200, "Red", token);

        // Same id, same tenant: a genuine duplicate, swallowed rather than surfaced, and the stored row
        // is left alone. The other half of the pair above — IgnoreDuplicates must not become "ignore
        // everything" just because tenancy is in the key.
        await theStore.Advanced.BulkInsertAsync(
            [new PennedNote { Id = SharedId, Owner = "Red", Body = "second", Revision = 9 }],
            BulkInsertMode.IgnoreDuplicates, 200, "Red", token);

        await using var redQuery = theStore.QuerySession("Red");
        var stored = await redQuery.LoadAsync<PennedNote>(SharedId, token);
        stored.ShouldNotBeNull()!.Body.ShouldBe("first");
        (await redQuery.Query<PennedNote>().CountAsync(token)).ShouldBe(1);
    }
}
