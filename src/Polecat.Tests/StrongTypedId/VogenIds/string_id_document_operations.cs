using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId.VogenIds;

// Mirrors Marten's ValueTypeTests/VogenIds/string_id_document_operations. String ids are
// externally assigned — there is no generation strategy — so there is no "store assigns the
// identity" case here.
[Collection("integration")]
public class string_id_document_operations : IntegrationContext
{
    public string_id_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "vogen_string"; });
    }

    private static VogenTeam Team(string id, string name = "Team") =>
        new() { Id = VogenTeamId.From(id), Name = name };

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Team(Guid.NewGuid().ToString(), "Smoke"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<VogenTeam>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        var team = Team("insert-1", "Inserted");
        await using var session = theStore.LightweightSession();
        session.Insert(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.LoadAsync<VogenTeam>("insert-1", TestContext.Current.CancellationToken))!
            .Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        var team = Team("update-1", "Original");
        await using var session = theStore.LightweightSession();
        session.Insert(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        team.Name = "Updated";
        session.Update(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenTeam>("update-1", TestContext.Current.CancellationToken))!
            .Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        var team = Team("load-1", "Load Me");
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<VogenTeam>("load-1", TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(team.Id);
        loaded.Name.ShouldBe("Load Me");
    }

    [Fact]
    public async Task load_many()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Team("many-1"), Team("many-2"), Team("many-3"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadManyAsync<VogenTeam>(
            ["many-1", "many-2", "many-3"], TestContext.Current.CancellationToken);

        loaded.Count.ShouldBe(3);
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        await using var session = theStore.IdentitySession();
        session.Store(Team("identity-1"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<VogenTeam>("identity-1", TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<VogenTeam>("identity-1", TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Team("delete-1"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<VogenTeam>("delete-1");
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenTeam>("delete-1", TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        var team = Team("delete-2");
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenTeam>("delete-2", TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        var team = Team("where-1", "LINQ Where");
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.Query<VogenTeam>()
            .Where(x => x.Id == team.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        loaded!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Team("order-1"), Team("order-2"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenTeam>()
            .OrderBy(x => x.Id)
            .Take(3)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_select_clause()
    {
        var team = Team("select-1");
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var ids = await session.Query<VogenTeam>()
            .Where(x => x.Id == team.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ids.Single().ShouldBe(team.Id);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        var one = Team("oneof-1");
        var two = Team("oneof-2");

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenTeam>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task bulk_insert()
    {
        var teams = Enumerable.Range(0, 5).Select(i => Team($"bulk-{i}", $"Bulk {i}")).ToList();

        await theStore.Advanced.BulkInsertAsync(teams, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        foreach (var team in teams)
        {
            (await query.LoadAsync<VogenTeam>(team.Id!.Value.Value, TestContext.Current.CancellationToken))
                .ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Team("exists-1"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<VogenTeam>("exists-1", TestContext.Current.CancellationToken)).ShouldBeTrue();
        (await query.CheckExistsAsync<VogenTeam>("nope", TestContext.Current.CancellationToken)).ShouldBeFalse();
    }
}
