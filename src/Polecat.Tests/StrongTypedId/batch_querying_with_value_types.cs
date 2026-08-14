using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.Tests.StrongTypedId.GeneratedIds;
using Polecat.Tests.StrongTypedId.VogenIds;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

// Mirrors Marten's ValueTypeTests/using_in_batch_queries and the batched half of
// StrongTypedId/check_exists_with_strong_typed_ids: strong-typed-id documents loaded and probed
// through a batched query, where every operation shares one round trip and each result has to be
// matched back to the right reader.
[Collection("integration")]
public class batch_querying_with_value_types : IntegrationContext
{
    public batch_querying_with_value_types(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "batch_value_types"; });
    }

    [Fact]
    public async Task load_one_at_a_time_in_a_batch()
    {
        var one = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "One" };
        var two = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Two" };
        var three = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Three" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two, three);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = session.CreateBatchQuery();
        var first = batch.Load<VogenInvoice>(one.Id!.Value.Value);
        var second = batch.Load<VogenInvoice>(two.Id!.Value.Value);
        var third = batch.Load<VogenInvoice>(three.Id!.Value.Value);

        await batch.Execute(TestContext.Current.CancellationToken);

        (await first)!.Name.ShouldBe("One");
        (await second)!.Name.ShouldBe("Two");
        (await third)!.Name.ShouldBe("Three");
    }

    [Fact]
    public async Task load_many_in_a_batch()
    {
        var one = new GeneratedInvoice { Id = new GeneratedInvoiceId(Guid.NewGuid()), Name = "One" };
        var two = new GeneratedInvoice { Id = new GeneratedInvoiceId(Guid.NewGuid()), Name = "Two" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = session.CreateBatchQuery();
        var loaded = batch.LoadMany<GeneratedInvoice>(one.Id.Value, two.Id.Value);

        await batch.Execute(TestContext.Current.CancellationToken);

        (await loaded).Count.ShouldBe(2);
    }

    [Fact]
    public async Task check_exists_in_a_batch_with_a_guid_wrapper()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = session.CreateBatchQuery();
        var hit = batch.CheckExists<VogenInvoice>(invoice.Id!.Value.Value);
        var miss = batch.CheckExists<VogenInvoice>(Guid.NewGuid());

        await batch.Execute(TestContext.Current.CancellationToken);

        (await hit).ShouldBeTrue();
        (await miss).ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_in_a_batch_with_an_int_wrapper()
    {
        var order = new VogenOrder { Id = VogenOrderId.From(5001), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(order);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = session.CreateBatchQuery();
        var hit = batch.CheckExists<VogenOrder>(5001);
        var miss = batch.CheckExists<VogenOrder>(999999);

        await batch.Execute(TestContext.Current.CancellationToken);

        (await hit).ShouldBeTrue();
        (await miss).ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_in_a_batch_with_a_long_wrapper()
    {
        var issue = new VogenIssue { Id = VogenIssueId.From(4001L), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(issue);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = session.CreateBatchQuery();
        var hit = batch.CheckExists<VogenIssue>(4001L);
        var miss = batch.CheckExists<VogenIssue>(999999L);

        await batch.Execute(TestContext.Current.CancellationToken);

        (await hit).ShouldBeTrue();
        (await miss).ShouldBeFalse();
    }

    [Fact]
    public async Task check_exists_in_a_batch_with_a_string_wrapper()
    {
        var team = new VogenTeam { Id = VogenTeamId.From("batch-exists"), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(team);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = session.CreateBatchQuery();
        var hit = batch.CheckExists<VogenTeam>("batch-exists");
        var miss = batch.CheckExists<VogenTeam>("nope");

        await batch.Execute(TestContext.Current.CancellationToken);

        (await hit).ShouldBeTrue();
        (await miss).ShouldBeFalse();
    }

    [Fact]
    public async Task query_by_a_wrapper_id_in_a_batch()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Queried" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = session.CreateBatchQuery();
        var matches = batch.Query<VogenInvoice>().Where(x => x.Id == invoice.Id).ToList();

        await batch.Execute(TestContext.Current.CancellationToken);

        (await matches).Single().Name.ShouldBe("Queried");
    }
}
