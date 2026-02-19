using Polecat.Tests.Harness;
using Polecat.Tests.Linq;

namespace Polecat.Tests.Session;

[Collection("integration")]
public class eject_operations : IntegrationContext
{
    public eject_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task eject_removes_pending_store_operation()
    {
        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "will-be-ejected" };

        theSession.Store(doc);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();

        theSession.Eject(doc);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
    }

    [Fact]
    public async Task eject_removes_pending_insert_operation()
    {
        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "will-be-ejected" };

        theSession.Insert(doc);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();

        theSession.Eject(doc);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
    }

    [Fact]
    public async Task eject_only_removes_the_specific_document()
    {
        var doc1 = new LinqTarget { Id = Guid.NewGuid(), Name = "keep" };
        var doc2 = new LinqTarget { Id = Guid.NewGuid(), Name = "eject" };

        theSession.Store(doc1);
        theSession.Store(doc2);
        theSession.PendingChanges.Operations.Count.ShouldBe(2);

        theSession.Eject(doc2);
        theSession.PendingChanges.Operations.Count.ShouldBe(1);
        theSession.PendingChanges.Operations[0].DocumentId!.ShouldBe(doc1.Id);
    }

    [Fact]
    public async Task ejected_document_is_not_saved()
    {
        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "ejected-no-save" };

        theSession.Store(doc);
        theSession.Eject(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<LinqTarget>(doc.Id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task eject_all_of_type_removes_all_operations_for_type()
    {
        var doc1 = new LinqTarget { Id = Guid.NewGuid(), Name = "target1" };
        var doc2 = new LinqTarget { Id = Guid.NewGuid(), Name = "target2" };
        var other = new EjectOtherDoc { Id = Guid.NewGuid(), Value = "other" };

        theSession.Store(doc1);
        theSession.Store(doc2);
        theSession.Store(other);

        theSession.PendingChanges.Operations.Count.ShouldBe(3);

        theSession.EjectAllOfType(typeof(LinqTarget));

        theSession.PendingChanges.Operations.Count.ShouldBe(1);
        theSession.PendingChanges.Operations[0].DocumentType.ShouldBe(typeof(EjectOtherDoc));
    }

    [Fact]
    public async Task eject_all_pending_changes_clears_everything()
    {
        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "pending" };
        theSession.Store(doc);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();

        theSession.EjectAllPendingChanges();
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
    }

    [Fact]
    public async Task eject_removes_from_identity_map()
    {
        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "identity-eject" };

        // Store and save using identity session
        await using var session = theStore.IdentitySession();
        session.Store(doc);
        await session.SaveChangesAsync();

        // Load to populate identity map
        var loaded1 = await session.LoadAsync<LinqTarget>(doc.Id);
        loaded1.ShouldNotBeNull();

        // Eject
        session.Eject(doc);

        // Loading again should fetch from database, not identity map
        var loaded2 = await session.LoadAsync<LinqTarget>(doc.Id);
        loaded2.ShouldNotBeNull();

        // Different reference since identity map entry was ejected
        ReferenceEquals(loaded1, loaded2).ShouldBeFalse();
    }

    [Fact]
    public async Task eject_all_of_type_clears_identity_map_for_type()
    {
        await using var session = theStore.IdentitySession();

        var doc1 = new LinqTarget { Id = Guid.NewGuid(), Name = "idmap1" };
        var doc2 = new LinqTarget { Id = Guid.NewGuid(), Name = "idmap2" };

        session.Store(doc1, doc2);
        await session.SaveChangesAsync();

        // Load to populate identity map
        var loaded1 = await session.LoadAsync<LinqTarget>(doc1.Id);
        loaded1.ShouldNotBeNull();

        // Eject all of type
        session.EjectAllOfType(typeof(LinqTarget));

        // Loading again should get new references
        var loaded1Again = await session.LoadAsync<LinqTarget>(doc1.Id);
        loaded1Again.ShouldNotBeNull();
        ReferenceEquals(loaded1, loaded1Again).ShouldBeFalse();
    }
}

public class EjectOtherDoc
{
    public Guid Id { get; set; }
    public string? Value { get; set; }
}
