using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId.VogenIds;

// Mirrors Marten's ValueTypeTests/VogenIds/long_based_document_operations.
[Collection("integration")]
public class long_based_document_operations : IntegrationContext
{
    public long_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "vogen_long"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var issue = new VogenIssue { Name = "Auto" };
        issue.Id.ShouldBeNull();

        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        issue.Id.ShouldNotBeNull();
        issue.Id!.Value.Value.ShouldBeGreaterThan(0L);
    }

    [Fact]
    public async Task store_assigns_hilo_ids()
    {
        var first = new VogenIssue { Name = "First" };
        var second = new VogenIssue { Name = "Second" };

        await using var session = theStore.LightweightSession();
        session.Store(first, second);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        first.Id!.Value.Value.ShouldNotBe(second.Id!.Value.Value);
    }

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new VogenIssue { Name = "Smoke" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<VogenIssue>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8001L), Name = "Inserted" };
        await using var session = theStore.LightweightSession();
        session.Insert(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.LoadAsync<VogenIssue>(8001L, TestContext.Current.CancellationToken))!.Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8002L), Name = "Original" };
        await using var session = theStore.LightweightSession();
        session.Insert(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        issue.Name = "Updated";
        session.Update(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenIssue>(8002L, TestContext.Current.CancellationToken))!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8003L), Name = "Load Me" };
        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<VogenIssue>(8003L, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(issue.Id);
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8004L), Name = "Identity" };
        await using var session = theStore.IdentitySession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<VogenIssue>(8004L, TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<VogenIssue>(8004L, TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8005L), Name = "Delete" };
        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<VogenIssue>(8005L);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenIssue>(8005L, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8006L), Name = "Delete Doc" };
        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenIssue>(8006L, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8007L), Name = "LINQ Where" };
        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.Query<VogenIssue>()
            .Where(x => x.Id == issue.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        loaded!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new VogenIssue { Id = VogenIssueId.From(8008L), Name = "A" });
        session.Store(new VogenIssue { Id = VogenIssueId.From(8009L), Name = "B" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenIssue>()
            .OrderBy(x => x.Id)
            .Take(3)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_select_clause()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8010L), Name = "Select" };
        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var ids = await session.Query<VogenIssue>()
            .Where(x => x.Id == issue.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ids.Single().ShouldBe(issue.Id);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        var one = new VogenIssue { Id = VogenIssueId.From(8011L), Name = "One" };
        var two = new VogenIssue { Id = VogenIssueId.From(8012L), Name = "Two" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenIssue>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task bulk_insert()
    {
        var issues = Enumerable.Range(8100, 5)
            .Select(i => new VogenIssue { Id = VogenIssueId.From(i), Name = $"Bulk {i}" })
            .ToList();

        await theStore.Advanced.BulkInsertAsync(issues, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        foreach (var issue in issues)
        {
            (await query.LoadAsync<VogenIssue>(issue.Id!.Value.Value, TestContext.Current.CancellationToken))
                .ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(8200L), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<VogenIssue>(8200L, TestContext.Current.CancellationToken)).ShouldBeTrue();
        (await query.CheckExistsAsync<VogenIssue>(999999L, TestContext.Current.CancellationToken)).ShouldBeFalse();
    }
}
