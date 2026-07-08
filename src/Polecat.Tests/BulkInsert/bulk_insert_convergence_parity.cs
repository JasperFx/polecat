using Polecat.Tests.Harness;

namespace Polecat.Tests.BulkInsert;

// Hierarchy types local to the bulk-insert convergence parity tests (#273 doc-side).
public abstract class BulkPerson
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class BulkEmployee : BulkPerson
{
    public string Department { get; set; } = string.Empty;
}

public class BulkContractor : BulkPerson
{
    public string Agency { get; set; } = string.Empty;
}

/// <summary>
///     Parity coverage for the columns the bespoke bulk-insert path SILENTLY OMITTED and the
///     closed-shape convergence now writes (#273 doc-side): the hierarchy <c>doc_type</c>
///     discriminator and the optimistic-concurrency <c>guid_version</c>. Each of these would
///     fail against the pre-convergence <c>BuildBatchCommand</c>.
/// </summary>
[Collection("integration")]
public class bulk_insert_convergence_parity : IntegrationContext
{
    public bulk_insert_convergence_parity(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task bulk_insert_hierarchy_writes_doc_type_discriminator()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "bulk_hierarchy";
            opts.Schema.For<BulkPerson>()
                .AddSubClass<BulkEmployee>()
                .AddSubClass<BulkContractor>();
        });

        var employee = new BulkEmployee { Id = Guid.NewGuid(), Name = "Emp", Department = "Eng" };
        var contractor = new BulkContractor { Id = Guid.NewGuid(), Name = "Con", Agency = "Acme" };

        await theStore.Advanced.BulkInsertAsync(new BulkPerson[] { employee, contractor });

        await using var query = theStore.QuerySession();

        // Without a persisted doc_type these loads could not resolve the concrete subclass.
        var asBase = await query.LoadAsync<BulkPerson>(employee.Id);
        asBase.ShouldBeOfType<BulkEmployee>();
        ((BulkEmployee)asBase).Department.ShouldBe("Eng");

        var asContractor = await query.LoadAsync<BulkContractor>(contractor.Id);
        asContractor.ShouldNotBeNull();
        asContractor.Agency.ShouldBe("Acme");

        // doc_type discrimination: loading a contractor row as the wrong subclass returns null.
        (await query.LoadAsync<BulkEmployee>(contractor.Id)).ShouldBeNull();
    }

    [Fact]
    public async Task bulk_insert_optimistic_doc_assigns_guid_version()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "bulk_optimistic");

        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "Optimistic" };

        await theStore.Advanced.BulkInsertAsync(new[] { doc });

        // The optimistic insert op stamps a fresh guid_version and applies it in place.
        doc.Version.ShouldNotBe(Guid.Empty);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<VersionedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        // A persisted guid_version round-trips back onto the reloaded document.
        loaded.Version.ShouldBe(doc.Version);
    }
}
