using Polecat.Tests.Harness;

namespace Polecat.Tests.Batching;

[Collection("integration")]
public class batch_query_operations : IntegrationContext
{
    public batch_query_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_batch_load_multiple_documents()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Alice", LastName = "A" };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Bob", LastName = "B" };
        var user3 = new User { Id = Guid.NewGuid(), FirstName = "Charlie", LastName = "C" };

        theSession.Store(user1, user2, user3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var loadUser1 = batch.Load<User>(user1.Id);
        var loadUser2 = batch.Load<User>(user2.Id);
        var loadUser3 = batch.Load<User>(user3.Id);

        await batch.Execute();

        var result1 = await loadUser1;
        var result2 = await loadUser2;
        var result3 = await loadUser3;

        result1.ShouldNotBeNull();
        result1.FirstName.ShouldBe("Alice");
        result2.ShouldNotBeNull();
        result2.FirstName.ShouldBe("Bob");
        result3.ShouldNotBeNull();
        result3.FirstName.ShouldBe("Charlie");
    }

    [Fact]
    public async Task batch_load_returns_null_for_missing()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var loadMissing = batch.Load<User>(Guid.NewGuid());

        await batch.Execute();

        var result = await loadMissing;
        result.ShouldBeNull();
    }

    [Fact]
    public async Task can_batch_load_many()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Alice" };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Bob" };
        var user3 = new User { Id = Guid.NewGuid(), FirstName = "Charlie" };

        theSession.Store(user1, user2, user3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var loadMany = batch.LoadMany<User>(user1.Id, user3.Id);

        await batch.Execute();

        var results = await loadMany;
        results.Count.ShouldBe(2);
        results.ShouldContain(u => u.FirstName == "Alice");
        results.ShouldContain(u => u.FirstName == "Charlie");
    }

    [Fact]
    public async Task can_batch_query_with_where()
    {
        var uniqueColor = $"BatchRed_{Guid.NewGuid():N}";
        var target1 = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 1 };
        var target2 = new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 2 };
        var target3 = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 3 };

        theSession.Store(target1, target2, target3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var redTargets = batch.Query<Target>()
            .Where(t => t.Color == uniqueColor)
            .ToList();

        await batch.Execute();

        var results = await redTargets;
        results.Count.ShouldBe(2);
        results.ShouldAllBe(t => t.Color == uniqueColor);
    }

    [Fact]
    public async Task can_batch_query_count()
    {
        var target1 = new Target { Id = Guid.NewGuid(), Color = "Green", Number = 10 };
        var target2 = new Target { Id = Guid.NewGuid(), Color = "Green", Number = 20 };
        var target3 = new Target { Id = Guid.NewGuid(), Color = "Yellow", Number = 30 };

        theSession.Store(target1, target2, target3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var greenCount = batch.Query<Target>()
            .Where(t => t.Color == "Green")
            .Count();

        await batch.Execute();

        var count = await greenCount;
        count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task can_batch_query_any()
    {
        var target = new Target { Id = Guid.NewGuid(), Color = "Purple", Number = 99 };

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var hasPurple = batch.Query<Target>()
            .Where(t => t.Color == "Purple")
            .Any();

        var hasOrange = batch.Query<Target>()
            .Where(t => t.Color == "OrangeNotExist")
            .Any();

        await batch.Execute();

        (await hasPurple).ShouldBeTrue();
        (await hasOrange).ShouldBeFalse();
    }

    [Fact]
    public async Task can_batch_query_first_or_default()
    {
        var target = new Target { Id = Guid.NewGuid(), Color = "Cyan", Number = 42 };

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var found = batch.Query<Target>()
            .Where(t => t.Color == "Cyan")
            .FirstOrDefault();

        var missing = batch.Query<Target>()
            .Where(t => t.Color == "MagentaNotExist")
            .FirstOrDefault();

        await batch.Execute();

        (await found).ShouldNotBeNull();
        (await found)!.Number.ShouldBe(42);
        (await missing).ShouldBeNull();
    }

    [Fact]
    public async Task can_mix_loads_and_queries_in_single_batch()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Diana" };
        var target = new Target { Id = Guid.NewGuid(), Color = "Silver", Number = 7 };

        theSession.Store(user);
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var loadUser = batch.Load<User>(user.Id);
        var silverTargets = batch.Query<Target>()
            .Where(t => t.Color == "Silver")
            .ToList();

        await batch.Execute();

        (await loadUser).ShouldNotBeNull();
        (await loadUser)!.FirstName.ShouldBe("Diana");
        (await silverTargets).Count.ShouldBeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task batch_with_string_id_load()
    {
        var doc = new StringDoc { Id = "batch-test-1", Name = "Batch Test" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var loaded = batch.Load<StringDoc>("batch-test-1");

        await batch.Execute();

        (await loaded).ShouldNotBeNull();
        (await loaded)!.Name.ShouldBe("Batch Test");
    }

    [Fact]
    public async Task execute_empty_batch_does_nothing()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // Should not throw
        await batch.Execute();
    }
}
