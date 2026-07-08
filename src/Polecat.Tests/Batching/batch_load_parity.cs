using JasperFx;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Batching;

// Hierarchy types local to the batched-load parity tests (#273 doc-side convergence).
public abstract class BatchPerson
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class BatchEmployee : BatchPerson
{
    public string Department { get; set; } = string.Empty;
}

public class BatchContractor : BatchPerson
{
    public string Agency { get; set; } = string.Empty;
}

/// <summary>
///     Broadened parity coverage for the batched Load/LoadMany read path after it was retired
///     onto the closed-shape QueryOnly storage + selector (#273 doc-side convergence). Exercises
///     the risk areas the shared read path must still honor: optimistic-concurrency + numeric
///     revision version sync, the soft-delete filter, the conjoined tenant filter (#234), and
///     hierarchy (subclass doc_type discrimination).
/// </summary>
[Collection("integration")]
public class batch_load_parity : IntegrationContext
{
    public batch_load_parity(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task batch_load_populates_guid_version()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "Optimistic" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();
        var savedVersion = doc.Version;
        savedVersion.ShouldNotBe(Guid.Empty);

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var loaded = batch.Load<VersionedDoc>(doc.Id);
        await batch.Execute();

        var result = await loaded;
        result.ShouldNotBeNull();
        result.Version.ShouldBe(savedVersion);
    }

    [Fact]
    public async Task batch_load_populates_numeric_revision()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "Revisioned" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var loaded = batch.Load<RevisionedDoc>(doc.Id);
        var loadedMany = batch.LoadMany<RevisionedDoc>(doc.Id);
        await batch.Execute();

        (await loaded).ShouldNotBeNull();
        (await loaded)!.Version.ShouldBe(1);
        (await loadedMany).Single().Version.ShouldBe(1);
    }

    [Fact]
    public async Task batch_load_respects_soft_delete_filter()
    {
        var live = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "Live" };
        var gone = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "Gone" };
        theSession.Store(live, gone);
        await theSession.SaveChangesAsync();

        theSession.Delete(gone); // soft delete
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var loadLive = batch.Load<SoftDeletedDoc>(live.Id);
        var loadGone = batch.Load<SoftDeletedDoc>(gone.Id);
        var loadMany = batch.LoadMany<SoftDeletedDoc>(live.Id, gone.Id);
        await batch.Execute();

        (await loadLive).ShouldNotBeNull();
        (await loadGone).ShouldBeNull();
        (await loadMany).Select(d => d.Id).ShouldBe(new[] { live.Id });
    }

    [Fact]
    public async Task batch_load_respects_conjoined_tenant_filter()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "batch_conjoined";
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        var id = Guid.NewGuid();
        theSession.ForTenant("tenant-a").Store(new TenantScopedDoc { Id = id, Name = "A" });
        theSession.ForTenant("tenant-b").Store(new TenantScopedDoc { Id = id, Name = "B" });
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession(new SessionOptions { TenantId = "tenant-a" });
        var batch = query.CreateBatchQuery();
        var loadOne = batch.Load<TenantScopedDoc>(id);
        var loadMany = batch.LoadMany<TenantScopedDoc>(id);
        await batch.Execute();

        (await loadOne).ShouldNotBeNull();
        (await loadOne)!.Name.ShouldBe("A");
        (await loadMany).Single().Name.ShouldBe("A");
    }

    [Fact]
    public async Task batch_load_discriminates_subclasses_by_doc_type()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "batch_hierarchy";
            opts.Schema.For<BatchPerson>()
                .AddSubClass<BatchEmployee>()
                .AddSubClass<BatchContractor>();
        });

        var employee = new BatchEmployee { Id = Guid.NewGuid(), Name = "Emp", Department = "Eng" };
        var contractor = new BatchContractor { Id = Guid.NewGuid(), Name = "Con", Agency = "Acme" };
        theSession.Store<BatchPerson>(employee, contractor);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var asBase = batch.Load<BatchPerson>(employee.Id);
        var asEmployee = batch.Load<BatchEmployee>(employee.Id);
        var wrongSubclass = batch.Load<BatchEmployee>(contractor.Id); // contractor row, filtered by doc_type
        var manyEmployees = batch.LoadMany<BatchEmployee>(employee.Id, contractor.Id);
        await batch.Execute();

        (await asBase).ShouldBeOfType<BatchEmployee>();
        (await asEmployee).ShouldNotBeNull();
        (await asEmployee)!.Department.ShouldBe("Eng");
        (await wrongSubclass).ShouldBeNull();
        (await manyEmployees).Select(e => e.Id).ShouldBe(new[] { employee.Id });
    }
}

/// <summary>Conjoined-tenancy target for the batch tenant-filter test.</summary>
public class TenantScopedDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
