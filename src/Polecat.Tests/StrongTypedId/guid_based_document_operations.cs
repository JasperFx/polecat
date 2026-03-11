using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

public record struct InvoiceId(Guid Value);

public class Invoice
{
    public InvoiceId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class guid_based_document_operations : IntegrationContext
{
    public guid_based_document_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "strong_guid"; });
    }

    [Fact]
    public async Task store_document_will_assign_the_identity()
    {
        var invoice = new Invoice { Name = "Auto" };
        invoice.Id.Value.ShouldBe(Guid.Empty);

        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        invoice.Id.Value.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        var invoice = new Invoice { Name = "Smoke" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.Query<Invoice>().AnyAsync()).ShouldBeTrue();
    }

    [Fact]
    public async Task insert_a_document_smoke_test()
    {
        var invoice = new Invoice { Name = "Inserted" };
        await using var session = theStore.LightweightSession();
        session.Insert(invoice);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Invoice>(invoice.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Inserted");
    }

    [Fact]
    public async Task update_a_document_smoke_test()
    {
        var invoice = new Invoice { Name = "Original" };
        await using var session = theStore.LightweightSession();
        session.Insert(invoice);
        await session.SaveChangesAsync();

        invoice.Name = "Updated";
        await using var session2 = theStore.LightweightSession();
        session2.Update(invoice);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Invoice>(invoice.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Updated");
    }

    [Fact]
    public async Task load_document()
    {
        var invoice = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "Load Me" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Invoice>(invoice.Id.Value);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(invoice.Id);
        loaded.Name.ShouldBe("Load Me");
    }

    [Fact]
    public async Task use_within_identity_map()
    {
        var invoice = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "Identity" };
        await using var session = theStore.IdentitySession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        var first = await session.LoadAsync<Invoice>(invoice.Id.Value);
        var second = await session.LoadAsync<Invoice>(invoice.Id.Value);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task delete_by_id()
    {
        var invoice = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "Delete" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete<Invoice>(invoice.Id.Value);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Invoice>(invoice.Id.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_document()
    {
        var invoice = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "Delete Doc" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(invoice);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Invoice>(invoice.Id.Value);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        var invoice = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "LINQ Where" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<Invoice>()
            .Where(x => x.Id == invoice.Id)
            .FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result!.Name.ShouldBe("LINQ Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "A" });
        session.Store(new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "B" });
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Invoice>()
            .OrderBy(x => x.Id)
            .ToListAsync();

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        var i1 = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "One" };
        var i2 = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "Two" };
        var i3 = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "Three" };

        await using var session = theStore.LightweightSession();
        session.Store(i1);
        session.Store(i2);
        session.Store(i3);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<Invoice>()
            .Where(x => x.Id.IsOneOf(i1.Id, i2.Id, i3.Id))
            .ToListAsync();

        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task bulk_insert()
    {
        var invoices = Enumerable.Range(0, 5)
            .Select(i => new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = $"Bulk {i}" })
            .ToList();

        await theStore.Advanced.BulkInsertAsync(invoices);

        await using var query = theStore.QuerySession();
        foreach (var invoice in invoices)
        {
            var loaded = await query.LoadAsync<Invoice>(invoice.Id.Value);
            loaded.ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task check_exists_with_wrapper_id()
    {
        var invoice = new Invoice { Id = new InvoiceId(Guid.NewGuid()), Name = "Exists" };
        await using var session = theStore.LightweightSession();
        session.Store(invoice);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.CheckExistsAsync<Invoice>(invoice.Id.Value)).ShouldBeTrue();
        (await query.CheckExistsAsync<Invoice>(Guid.NewGuid())).ShouldBeFalse();
    }
}