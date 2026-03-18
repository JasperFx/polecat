using Polecat.Linq;
using Polecat.Linq.Metadata;
using Polecat.Metadata;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Metadata;

public class CreatedDoc : ICreated
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTimeOffset CreatedAt { get; set; }
}

public class PlainDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class created_at_tests : IntegrationContext
{
    public created_at_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task created_at_is_populated_on_load_for_icreated()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "created_at_load");

        var doc = new CreatedDoc { Id = Guid.NewGuid(), Name = "test" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<CreatedDoc>(doc.Id);

        loaded.ShouldNotBeNull();
        loaded.CreatedAt.ShouldNotBe(default);
        loaded.CreatedAt.ShouldBeGreaterThan(DateTimeOffset.UtcNow.AddMinutes(-1));
    }

    [Fact]
    public async Task created_at_does_not_change_on_update()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "created_at_update");

        var doc = new CreatedDoc { Id = Guid.NewGuid(), Name = "original" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // Load to get created_at
        await using var query1 = theStore.QuerySession();
        var loaded1 = await query1.LoadAsync<CreatedDoc>(doc.Id);
        loaded1.ShouldNotBeNull();
        var originalCreatedAt = loaded1.CreatedAt;
        originalCreatedAt.ShouldNotBe(default);

        // Wait briefly and update
        await Task.Delay(50);

        await using var session2 = theStore.LightweightSession();
        loaded1.Name = "updated";
        session2.Store(loaded1);
        await session2.SaveChangesAsync();

        // Load again and verify created_at unchanged
        await using var query2 = theStore.QuerySession();
        var loaded2 = await query2.LoadAsync<CreatedDoc>(doc.Id);
        loaded2.ShouldNotBeNull();
        loaded2.CreatedAt.ShouldBe(originalCreatedAt);
    }

    [Fact]
    public async Task created_at_populated_in_load_many()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "created_at_many");

        var doc1 = new CreatedDoc { Id = Guid.NewGuid(), Name = "one" };
        var doc2 = new CreatedDoc { Id = Guid.NewGuid(), Name = "two" };
        theSession.Store(doc1);
        theSession.Store(doc2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<CreatedDoc>(new[] { doc1.Id, doc2.Id });

        loaded.Count.ShouldBe(2);
        loaded.ShouldAllBe(d => d.CreatedAt != default);
    }

    [Fact]
    public async Task created_since_linq_filter()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "created_at_since");

        var before = DateTimeOffset.UtcNow.AddSeconds(-1);

        var doc = new CreatedDoc { Id = Guid.NewGuid(), Name = "recent" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<CreatedDoc>()
            .CreatedSince(before)
            .ToListAsync();

        results.ShouldContain(d => d.Id == doc.Id);
    }

    [Fact]
    public async Task created_before_linq_filter()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "created_at_before");

        var doc = new CreatedDoc { Id = Guid.NewGuid(), Name = "older" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        var after = DateTimeOffset.UtcNow.AddSeconds(1);

        await using var query = theStore.QuerySession();
        var results = await query.Query<CreatedDoc>()
            .CreatedBefore(after)
            .ToListAsync();

        results.ShouldContain(d => d.Id == doc.Id);
    }

    [Fact]
    public async Task plain_doc_without_icreated_still_works()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "created_at_plain");

        var doc = new PlainDoc { Id = Guid.NewGuid(), Name = "no interface" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<PlainDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Name.ShouldBe("no interface");
    }
}
