using Polecat.Tests.Harness;

namespace Polecat.Tests.Documents;

[Collection("integration")]
public class CheckDocumentExistsTests : IntegrationContext
{
    public CheckDocumentExistsTests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task check_exists_by_guid_id_hit()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Exists", LastName = "Test" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<User>(user.Id);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task check_exists_by_guid_id_miss()
    {
        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<User>(Guid.NewGuid());
        exists.ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_by_string_id_hit()
    {
        var doc = new StringDoc { Id = "exists-test-" + Guid.NewGuid(), Name = "Test" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<StringDoc>(doc.Id);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task check_exists_by_string_id_miss()
    {
        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<StringDoc>("nonexistent-" + Guid.NewGuid());
        exists.ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_by_int_id_hit()
    {
        var doc = new IntDoc { Name = "Exists" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<IntDoc>(doc.Id);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task check_exists_by_int_id_miss()
    {
        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<IntDoc>(999999);
        exists.ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_by_long_id_hit()
    {
        var doc = new LongDoc { Name = "Exists" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<LongDoc>(doc.Id);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task check_exists_by_long_id_miss()
    {
        await using var query = theStore.QuerySession();
        var exists = await query.CheckExistsAsync<LongDoc>(999999L);
        exists.ShouldBeFalse();
    }
}

[Collection("integration")]
public class CheckDocumentExistsInBatchTests : IntegrationContext
{
    public CheckDocumentExistsInBatchTests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task check_exists_in_batch_by_guid_id()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Batch", LastName = "Test" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var existsHit = batch.CheckExists<User>(user.Id);
        var existsMiss = batch.CheckExists<User>(Guid.NewGuid());
        await batch.Execute();

        (await existsHit).ShouldBeTrue();
        (await existsMiss).ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_in_batch_by_string_id()
    {
        var doc = new StringDoc { Id = "batch-exists-" + Guid.NewGuid(), Name = "Test" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var existsHit = batch.CheckExists<StringDoc>(doc.Id);
        var existsMiss = batch.CheckExists<StringDoc>("nope-" + Guid.NewGuid());
        await batch.Execute();

        (await existsHit).ShouldBeTrue();
        (await existsMiss).ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_in_batch_by_int_id()
    {
        var doc = new IntDoc { Name = "Batch" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var existsHit = batch.CheckExists<IntDoc>(doc.Id);
        var existsMiss = batch.CheckExists<IntDoc>(888888);
        await batch.Execute();

        (await existsHit).ShouldBeTrue();
        (await existsMiss).ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_in_batch_by_long_id()
    {
        var doc = new LongDoc { Name = "Batch" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var existsHit = batch.CheckExists<LongDoc>(doc.Id);
        var existsMiss = batch.CheckExists<LongDoc>(777777L);
        await batch.Execute();

        (await existsHit).ShouldBeTrue();
        (await existsMiss).ShouldBeFalse();
    }
}