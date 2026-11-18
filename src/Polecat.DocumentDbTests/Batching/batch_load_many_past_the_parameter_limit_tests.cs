using JasperFx;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Batching;

/// <summary>
///     #834. The batched <c>LoadMany</c> bound one parameter per id, so it was refused past 2098 ids
///     while <c>LoadManyAsync</c> over the very same list, bound as one JSON array since #363, was not.
/// </summary>
public class batch_load_many_past_the_parameter_limit_tests : OneOffConfigurationsContext
{
    private const int Decoys = 3000;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public class GuidDoc
    {
        public Guid Id { get; set; }
    }

    public class StringDoc
    {
        public string Id { get; set; } = "";
    }

    public class TenantedDoc
    {
        public Guid Id { get; set; }
    }

    public class Person
    {
        public Guid Id { get; set; }
    }

    public class Teacher : Person;

    public class Student : Person;

    [Fact]
    public async Task guid_ids_load()
    {
        var wanted = new GuidDoc { Id = Guid.NewGuid() };

        await using var session = theStore.LightweightSession();
        session.Store(wanted);
        await session.SaveChangesAsync(Ct);

        var batch = session.CreateBatchQuery();
        var ids = Enumerable.Range(0, Decoys).Select(_ => Guid.NewGuid()).Append(wanted.Id).ToArray();
        var loaded = batch.LoadMany<GuidDoc>(ids);
        await batch.Execute(Ct);

        (await loaded).ShouldHaveSingleItem().Id.ShouldBe(wanted.Id);
    }

    [Fact]
    public async Task string_ids_load()
    {
        var wanted = new StringDoc { Id = "wanted" };

        await using var session = theStore.LightweightSession();
        session.Store(wanted);
        await session.SaveChangesAsync(Ct);

        var batch = session.CreateBatchQuery();
        var ids = Enumerable.Range(0, Decoys).Select(i => $"decoy-{i}").Append("wanted").ToArray();
        var loaded = batch.LoadMany<StringDoc>(ids);
        await batch.Execute(Ct);

        (await loaded).ShouldHaveSingleItem().Id.ShouldBe("wanted");
    }

    [Fact]
    public async Task the_soft_delete_filter_survives()
    {
        var kept = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "kept" };
        var gone = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "gone" };

        await using var session = theStore.LightweightSession();
        session.Store(kept);
        session.Store(gone);
        await session.SaveChangesAsync(Ct);

        session.Delete(gone);
        await session.SaveChangesAsync(Ct);

        var batch = session.CreateBatchQuery();
        var loaded = batch.LoadMany<SoftDeletedDoc>(Enumerable.Range(0, Decoys).Select(_ => Guid.NewGuid())
            .Append(kept.Id).Append(gone.Id).ToArray());
        await batch.Execute(Ct);

        (await loaded).ShouldHaveSingleItem().Id.ShouldBe(kept.Id);
    }

    [Fact]
    public async Task the_conjoined_tenant_filter_survives()
    {
        ConfigureStore(opts => opts.Schema.For<TenantedDoc>().MultiTenanted());

        var doc = new TenantedDoc { Id = Guid.NewGuid() };

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Store(doc);
            await session.SaveChangesAsync(Ct);
        }

        var ids = Enumerable.Range(0, Decoys).Select(_ => Guid.NewGuid()).Append(doc.Id).ToArray();

        await using var alpha = theStore.QuerySession(new SessionOptions { TenantId = "alpha" });
        var alphaBatch = alpha.CreateBatchQuery();
        var alphaLoad = alphaBatch.LoadMany<TenantedDoc>(ids);
        await alphaBatch.Execute(Ct);
        (await alphaLoad).ShouldHaveSingleItem();

        await using var beta = theStore.QuerySession(new SessionOptions { TenantId = "beta" });
        var betaBatch = beta.CreateBatchQuery();
        var betaLoad = betaBatch.LoadMany<TenantedDoc>(ids);
        await betaBatch.Execute(Ct);
        (await betaLoad).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_subclass_load_keeps_its_discriminator()
    {
        ConfigureStore(opts => opts.Schema.For<Person>().AddSubClass<Teacher>().AddSubClass<Student>());

        var teacher = new Teacher { Id = Guid.NewGuid() };
        var student = new Student { Id = Guid.NewGuid() };

        await using var session = theStore.LightweightSession();
        session.Store(teacher);
        session.Store(student);
        await session.SaveChangesAsync(Ct);

        var batch = session.CreateBatchQuery();
        var loaded = batch.LoadMany<Teacher>(Enumerable.Range(0, Decoys).Select(_ => Guid.NewGuid())
            .Append(teacher.Id).Append(student.Id).ToArray());
        await batch.Execute(Ct);

        (await loaded).ShouldHaveSingleItem().Id.ShouldBe(teacher.Id);
    }

    /// <summary>
    ///     Not a change, a pin. Each document write has always been its own command, and the limit is
    ///     per command, so a session of thousands of writes was never exposed to #834. If a write ever
    ///     stops being its own command, this is the test that says so.
    /// </summary>
    [Fact]
    public async Task a_save_of_thousands_of_documents_is_not_limited_by_parameters()
    {
        var docs = Enumerable.Range(0, 5000).Select(_ => new GuidDoc { Id = Guid.NewGuid() }).ToArray();

        await using var session = theStore.LightweightSession();
        foreach (var doc in docs) session.Store(doc);
        await session.SaveChangesAsync(Ct);

        (await session.LoadManyAsync<GuidDoc>(docs.Select(x => x.Id).ToArray(), Ct)).Count.ShouldBe(5000);
    }
}
