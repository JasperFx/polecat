using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId.VogenIds;

// Mirrors Marten's ValueTypeTests/VogenIds/guid_based_document_operations.
[Collection("integration")]
public class guid_based_document_operations : IntegrationContext
{
    public guid_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "vogen_guid"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var invoice = new VogenInvoice { Name = "Auto" };
        invoice.Id.ShouldBeNull();

        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        invoice.Id.ShouldNotBeNull();
        invoice.Id!.Value.Value.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new VogenInvoice { Name = "Smoke" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<VogenInvoice>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Inserted" };
        await using var session = theStore.LightweightSession();
        session.Insert(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Original" };
        await using var session = theStore.LightweightSession();
        session.Insert(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        invoice.Name = "Updated";
        session.Update(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken);
        loaded!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Load Me" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(invoice.Id);
        loaded.Name.ShouldBe("Load Me");
    }

    [Fact]
    public async Task load_many()
    {
        var one = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "One" };
        var two = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Two" };
        var three = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Three" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two, three);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadManyAsync<VogenInvoice>(
            [one.Id!.Value.Value, two.Id!.Value.Value, three.Id!.Value.Value],
            TestContext.Current.CancellationToken);

        loaded.Count.ShouldBe(3);
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Identity" };
        await using var session = theStore.IdentitySession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var first = await session.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken);
        var second = await session.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Delete" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<VogenInvoice>(invoice.Id!.Value.Value);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken))
            .ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Delete Doc" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken))
            .ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "LINQ Where" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.Query<VogenInvoice>()
            .Where(x => x.Id == invoice.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "A" });
        session.Store(new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "B" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenInvoice>()
            .OrderBy(x => x.Id)
            .Take(3)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_select_clause()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Select" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var ids = await session.Query<VogenInvoice>()
            .Where(x => x.Id == invoice.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ids.Single().ShouldBe(invoice.Id);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        var one = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "One" };
        var two = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Two" };

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<VogenInvoice>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task bulk_insert()
    {
        var invoices = Enumerable.Range(0, 5)
            .Select(i => new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = $"Bulk {i}" })
            .ToList();

        await theStore.Advanced.BulkInsertAsync(invoices, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        foreach (var invoice in invoices)
        {
            (await query.LoadAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken))
                .ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        var invoice = new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<VogenInvoice>(invoice.Id!.Value.Value, TestContext.Current.CancellationToken))
            .ShouldBeTrue();
        (await query.CheckExistsAsync<VogenInvoice>(Guid.NewGuid(), TestContext.Current.CancellationToken))
            .ShouldBeFalse();
    }
}
