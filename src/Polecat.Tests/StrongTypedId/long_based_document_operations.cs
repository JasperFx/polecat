using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

public record struct IssueId(long Value);

public class Issue
{
    public IssueId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class long_based_document_operations : IntegrationContext
{
    public long_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "strong_long"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var issue = new Issue { Name = "Auto" };
        issue.Id.Value.ShouldBe(0L);

        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync();

        issue.Id.Value.ShouldBeGreaterThan(0L);
    }

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "Smoke" };
        session.Store(issue);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.Query<Issue>().AnyAsync()).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "Inserted" };
        session.Store(issue);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Issue>(issue.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "Original" };
        session.Store(issue);
        await session.SaveChangesAsync();

        issue.Name = "Updated";
        await using var session2 = theStore.LightweightSession();
        session2.Update(issue);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Issue>(issue.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "Load Me" };
        session.Store(issue);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Issue>(issue.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(issue.Id);
        loaded.Name.ShouldBe("Load Me");
    }

    [Fact]
    public async Task store_assigns_hilo_ids()
    {
        await using var session = theStore.LightweightSession();
        var issues = new List<Issue>();
        for (var i = 0; i < 5; i++)
        {
            var issue = new Issue { Name = $"Issue {i}" };
            session.Store(issue);
            issues.Add(issue);
        }

        await session.SaveChangesAsync();

        foreach (var issue in issues)
        {
            issue.Id.Value.ShouldBeGreaterThan(0L);
        }

        issues.Select(i => i.Id.Value).Distinct().Count().ShouldBe(5);
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        await using var session = theStore.IdentitySession();
        var issue = new Issue { Name = "Identity" };
        session.Store(issue);
        await session.SaveChangesAsync();

        var first = await session.LoadAsync<Issue>(issue.Id.Value);
        var second = await session.LoadAsync<Issue>(issue.Id.Value);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "Delete" };
        session.Store(issue);
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete<Issue>(issue.Id.Value);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Issue>(issue.Id.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "Delete Doc" };
        session.Store(issue);
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(issue);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Issue>(issue.Id.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "LINQ Where" };
        session.Store(issue);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<Issue>()
            .Where(x => x.Id == issue.Id)
            .FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new Issue { Name = "A" });
        session.Store(new Issue { Name = "B" });
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Issue>()
            .OrderBy(x => x.Id)
            .ToListAsync();

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        await using var session = theStore.LightweightSession();
        var i1 = new Issue { Name = "One" };
        var i2 = new Issue { Name = "Two" };
        var i3 = new Issue { Name = "Three" };
        session.Store(i1);
        session.Store(i2);
        session.Store(i3);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Issue>()
            .Where(x => x.Id.IsOneOf(i1.Id, i2.Id, i3.Id))
            .ToListAsync();

        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task bulk_insert()
    {
        var issues = Enumerable.Range(0, 5)
            .Select(i => new Issue { Name = $"Bulk {i}" })
            .ToList();

        await theStore.Advanced.BulkInsertAsync(issues);

        await using var query = theStore.QuerySession();
        foreach (var issue in issues)
        {
            var loaded = await query.LoadAsync<Issue>(issue.Id.Value);
            loaded.ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        await using var session = theStore.LightweightSession();
        var issue = new Issue { Name = "Exists" };
        session.Store(issue);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<Issue>(issue.Id.Value)).ShouldBeTrue();
        (await query.CheckExistsAsync<Issue>(999999L)).ShouldBeFalse();
    }
}
