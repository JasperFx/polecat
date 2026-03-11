using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

public record struct TeamId(string Value);

public class Team
{
    public TeamId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class string_based_document_operations : IntegrationContext
{
    public string_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "strong_string"; });
    }

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        var team = new Team { Id = new TeamId("team-" + Guid.NewGuid()), Name = "Smoke" };
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.Query<Team>().AnyAsync()).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        var team = new Team { Id = new TeamId("team-insert-" + Guid.NewGuid()), Name = "Inserted" };
        await using var session = theStore.LightweightSession();
        session.Insert(team);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Team>(team.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        var team = new Team { Id = new TeamId("team-update-" + Guid.NewGuid()), Name = "Original" };
        await using var session = theStore.LightweightSession();
        session.Insert(team);
        await session.SaveChangesAsync();

        team.Name = "Updated";
        await using var session2 = theStore.LightweightSession();
        session2.Update(team);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Team>(team.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        var team = new Team { Id = new TeamId("team-load-" + Guid.NewGuid()), Name = "Load Me" };
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Team>(team.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(team.Id);
        loaded.Name.ShouldBe("Load Me");
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        var team = new Team { Id = new TeamId("team-idmap-" + Guid.NewGuid()), Name = "Identity" };
        await using var session = theStore.IdentitySession();
        session.Store(team);
        await session.SaveChangesAsync();

        var first = await session.LoadAsync<Team>(team.Id.Value);
        var second = await session.LoadAsync<Team>(team.Id.Value);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        var team = new Team { Id = new TeamId("team-del-" + Guid.NewGuid()), Name = "Delete" };
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete<Team>(team.Id.Value);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Team>(team.Id.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        var team = new Team { Id = new TeamId("team-deldoc-" + Guid.NewGuid()), Name = "Delete Doc" };
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(team);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Team>(team.Id.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        var team = new Team { Id = new TeamId("team-linq-" + Guid.NewGuid()), Name = "LINQ Where" };
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<Team>()
            .Where(x => x.Id == team.Id)
            .FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new Team { Id = new TeamId("team-a-" + Guid.NewGuid()), Name = "A" });
        session.Store(new Team { Id = new TeamId("team-b-" + Guid.NewGuid()), Name = "B" });
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Team>()
            .OrderBy(x => x.Id)
            .ToListAsync();

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        var t1 = new Team { Id = new TeamId("team-one-" + Guid.NewGuid()), Name = "One" };
        var t2 = new Team { Id = new TeamId("team-two-" + Guid.NewGuid()), Name = "Two" };
        var t3 = new Team { Id = new TeamId("team-three-" + Guid.NewGuid()), Name = "Three" };

        await using var session = theStore.LightweightSession();
        session.Store(t1);
        session.Store(t2);
        session.Store(t3);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Team>()
            .Where(x => x.Id.IsOneOf(t1.Id, t2.Id, t3.Id))
            .ToListAsync();

        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task bulk_insert()
    {
        var teams = Enumerable.Range(0, 5)
            .Select(i => new Team { Id = new TeamId($"team-bulk-{i}-{Guid.NewGuid()}"), Name = $"Bulk {i}" })
            .ToList();

        await theStore.Advanced.BulkInsertAsync(teams);

        await using var query = theStore.QuerySession();
        foreach (var team in teams)
        {
            var loaded = await query.LoadAsync<Team>(team.Id.Value);
            loaded.ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        var team = new Team { Id = new TeamId("team-exists-" + Guid.NewGuid()), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<Team>(team.Id.Value)).ShouldBeTrue();
        (await query.CheckExistsAsync<Team>("nonexistent-" + Guid.NewGuid())).ShouldBeFalse();
    }
}
