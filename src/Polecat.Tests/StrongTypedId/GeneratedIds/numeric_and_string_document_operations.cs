using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId.GeneratedIds;

// Mirrors Marten's ValueTypeTests/StrongTypedId int_/long_/string_id_document_operations. Grouped
// into one class per inner type family because the generated ids differ only in what they wrap.
[Collection("integration")]
public class int_based_document_operations : IntegrationContext
{
    public int_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "stid_int"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var order = new GeneratedOrder { Name = "Auto" };
        order.Id.Value.ShouldBe(0);

        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        order.Id.Value.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task store_assigns_hilo_ids()
    {
        var first = new GeneratedOrder { Name = "First" };
        var second = new GeneratedOrder { Name = "Second" };

        await using var session = theStore.LightweightSession();
        session.Store(first, second);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        first.Id.Value.ShouldNotBe(second.Id.Value);
    }

    [Fact]
    public async Task insert_update_and_load()
    {
        var order = new GeneratedOrder { Id = new GeneratedOrderId(7001), Name = "Original" };
        await using var session = theStore.LightweightSession();
        session.Insert(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        order.Name = "Updated";
        session.Update(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<GeneratedOrder>(7001, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(order.Id);
        loaded.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        await using var session = theStore.IdentitySession();
        session.Store(new GeneratedOrder { Id = new GeneratedOrderId(7002), Name = "Identity" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<GeneratedOrder>(7002, TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<GeneratedOrder>(7002, TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id_and_by_document()
    {
        var byId = new GeneratedOrder { Id = new GeneratedOrderId(7003), Name = "By Id" };
        var byDoc = new GeneratedOrder { Id = new GeneratedOrderId(7004), Name = "By Doc" };

        await using var session = theStore.LightweightSession();
        session.Store(byId, byDoc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<GeneratedOrder>(7003);
        session.Delete(byDoc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<GeneratedOrder>(7003, TestContext.Current.CancellationToken)).ShouldBeNull();
        (await query.LoadAsync<GeneratedOrder>(7004, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_order_select_and_is_one_of()
    {
        var one = new GeneratedOrder { Id = new GeneratedOrderId(7005), Name = "One" };
        var two = new GeneratedOrder { Id = new GeneratedOrderId(7006), Name = "Two" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<GeneratedOrder>()
            .Where(x => x.Id == one.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken))!.Name.ShouldBe("One");

        var ordered = await session.Query<GeneratedOrder>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .OrderBy(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        ordered.Select(x => x.Name).ShouldBe(["One", "Two"]);

        var ids = await session.Query<GeneratedOrder>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .OrderBy(x => x.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        ids.ShouldBe([one.Id, two.Id]);
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new GeneratedOrder { Id = new GeneratedOrderId(7007), Name = "Exists" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<GeneratedOrder>(7007, TestContext.Current.CancellationToken)).ShouldBeTrue();
        (await query.CheckExistsAsync<GeneratedOrder>(999999, TestContext.Current.CancellationToken)).ShouldBeFalse();
    }
}

[Collection("integration")]
public class long_based_document_operations : IntegrationContext
{
    public long_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "stid_long"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var issue = new GeneratedIssue { Name = "Auto" };
        issue.Id.Value.ShouldBe(0L);

        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        issue.Id.Value.ShouldBeGreaterThan(0L);
    }

    [Fact]
    public async Task store_assigns_hilo_ids()
    {
        var first = new GeneratedIssue { Name = "First" };
        var second = new GeneratedIssue { Name = "Second" };

        await using var session = theStore.LightweightSession();
        session.Store(first, second);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        first.Id.Value.ShouldNotBe(second.Id.Value);
    }

    [Fact]
    public async Task insert_update_and_load()
    {
        var issue = new GeneratedIssue { Id = new GeneratedIssueId(6001L), Name = "Original" };
        await using var session = theStore.LightweightSession();
        session.Insert(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        issue.Name = "Updated";
        session.Update(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<GeneratedIssue>(6001L, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(issue.Id);
        loaded.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        await using var session = theStore.IdentitySession();
        session.Store(new GeneratedIssue { Id = new GeneratedIssueId(6002L), Name = "Identity" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<GeneratedIssue>(6002L, TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<GeneratedIssue>(6002L, TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id_and_by_document()
    {
        var byId = new GeneratedIssue { Id = new GeneratedIssueId(6003L), Name = "By Id" };
        var byDoc = new GeneratedIssue { Id = new GeneratedIssueId(6004L), Name = "By Doc" };

        await using var session = theStore.LightweightSession();
        session.Store(byId, byDoc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<GeneratedIssue>(6003L);
        session.Delete(byDoc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<GeneratedIssue>(6003L, TestContext.Current.CancellationToken)).ShouldBeNull();
        (await query.LoadAsync<GeneratedIssue>(6004L, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_order_select_and_is_one_of()
    {
        var one = new GeneratedIssue { Id = new GeneratedIssueId(6005L), Name = "One" };
        var two = new GeneratedIssue { Id = new GeneratedIssueId(6006L), Name = "Two" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<GeneratedIssue>()
            .Where(x => x.Id == one.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken))!.Name.ShouldBe("One");

        var ordered = await session.Query<GeneratedIssue>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .OrderBy(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        ordered.Select(x => x.Name).ShouldBe(["One", "Two"]);

        var ids = await session.Query<GeneratedIssue>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .OrderBy(x => x.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        ids.ShouldBe([one.Id, two.Id]);
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new GeneratedIssue { Id = new GeneratedIssueId(6007L), Name = "Exists" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<GeneratedIssue>(6007L, TestContext.Current.CancellationToken)).ShouldBeTrue();
        (await query.CheckExistsAsync<GeneratedIssue>(999999L, TestContext.Current.CancellationToken)).ShouldBeFalse();
    }
}

[Collection("integration")]
public class string_id_document_operations : IntegrationContext
{
    public string_id_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "stid_string"; });
    }

    private static GeneratedTeam Team(string id, string name = "Team") =>
        new() { Id = new GeneratedTeamId(id), Name = name };

    [Fact]
    public async Task insert_update_and_load()
    {
        var team = Team("g-update-1", "Original");
        await using var session = theStore.LightweightSession();
        session.Insert(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        team.Name = "Updated";
        session.Update(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<GeneratedTeam>("g-update-1", TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(team.Id);
        loaded.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_many()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Team("g-many-1"), Team("g-many-2"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadManyAsync<GeneratedTeam>(
            ["g-many-1", "g-many-2"], TestContext.Current.CancellationToken);
        loaded.Count.ShouldBe(2);
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        await using var session = theStore.IdentitySession();
        session.Store(Team("g-identity-1"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<GeneratedTeam>("g-identity-1", TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<GeneratedTeam>("g-identity-1", TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id_and_by_document()
    {
        var byDoc = Team("g-delete-2");
        await using var session = theStore.LightweightSession();
        session.Store(Team("g-delete-1"), byDoc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<GeneratedTeam>("g-delete-1");
        session.Delete(byDoc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<GeneratedTeam>("g-delete-1", TestContext.Current.CancellationToken)).ShouldBeNull();
        (await query.LoadAsync<GeneratedTeam>("g-delete-2", TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_order_select_and_is_one_of()
    {
        var one = Team("g-linq-1", "One");
        var two = Team("g-linq-2", "Two");

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<GeneratedTeam>()
            .Where(x => x.Id == one.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken))!.Name.ShouldBe("One");

        var ordered = await session.Query<GeneratedTeam>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .OrderBy(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        ordered.Select(x => x.Name).ShouldBe(["One", "Two"]);

        var ids = await session.Query<GeneratedTeam>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .OrderBy(x => x.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        ids.ShouldBe([one.Id, two.Id]);
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Team("g-exists-1"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<GeneratedTeam>("g-exists-1", TestContext.Current.CancellationToken))
            .ShouldBeTrue();
        (await query.CheckExistsAsync<GeneratedTeam>("nope", TestContext.Current.CancellationToken))
            .ShouldBeFalse();
    }
}
