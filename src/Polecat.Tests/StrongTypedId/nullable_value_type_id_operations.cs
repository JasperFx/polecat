using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Shouldly;
using StronglyTypedIds;

namespace Polecat.Tests.StrongTypedId;

// Mirrors Marten's paired ValueTypeTests/StrongTypedId/string_id_document_operations classes: the
// same wrapper used as `Id` both nullable and non-nullable. Marten documents the nullable form as the
// way to let the store see "no identity yet" without materializing a zero-valued wrapper, and it is
// mandatory for Vogen, which forbids an uninitialized value object. Both forms have to map to the
// same column and the same LINQ translation.

[StronglyTypedId(Template.String)]
public readonly partial struct SquadId;

public class NullableSquad
{
    public SquadId? Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class NonNullableSquad
{
    public SquadId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record struct RosterId(Guid Value);

public class NullableRoster
{
    public RosterId? Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class nullable_value_type_id_operations : IntegrationContext
{
    public nullable_value_type_id_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "nullable_value_id"; });
    }

    [Theory]
    [InlineData(typeof(NullableSquad), typeof(string))]
    [InlineData(typeof(NonNullableSquad), typeof(string))]
    [InlineData(typeof(NullableRoster), typeof(Guid))]
    public void nullable_and_non_nullable_map_to_the_same_inner_type(Type documentType, Type expectedInner)
    {
        var mapping = new DocumentMapping(documentType, new StoreOptions());
        mapping.IsStrongTypedId.ShouldBeTrue();
        mapping.InnerIdType.ShouldBe(expectedInner);
    }

    [Fact]
    public async Task round_trip_a_nullable_string_wrapper_id()
    {
        var squad = new NullableSquad { Id = new SquadId("squad-1"), Name = "Nullable" };
        await using var session = theStore.LightweightSession();
        session.Store(squad);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<NullableSquad>("squad-1", TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(squad.Id);
        loaded.Name.ShouldBe("Nullable");
    }

    [Fact]
    public async Task round_trip_a_non_nullable_string_wrapper_id()
    {
        var squad = new NonNullableSquad { Id = new SquadId("squad-2"), Name = "Non nullable" };
        await using var session = theStore.LightweightSession();
        session.Store(squad);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<NonNullableSquad>("squad-2", TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(squad.Id);
    }

    [Fact]
    public async Task assign_an_identity_to_a_nullable_guid_wrapper()
    {
        var roster = new NullableRoster { Name = "Auto" };
        roster.Id.ShouldBeNull();

        await using var session = theStore.LightweightSession();
        session.Store(roster);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        roster.Id.ShouldNotBeNull();
        roster.Id!.Value.Value.ShouldNotBe(Guid.Empty);

        var loaded = await session.LoadAsync<NullableRoster>(
            roster.Id!.Value.Value, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(roster.Id);
    }

    [Fact]
    public async Task query_by_a_nullable_wrapper_id()
    {
        var roster = new NullableRoster { Id = new RosterId(Guid.NewGuid()), Name = "Queried" };
        await using var session = theStore.LightweightSession();
        session.Store(roster);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.Query<NullableRoster>()
            .Where(x => x.Id == roster.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded!.Name.ShouldBe("Queried");
    }

    [Fact]
    public async Task select_a_nullable_wrapper_id()
    {
        var roster = new NullableRoster { Id = new RosterId(Guid.NewGuid()), Name = "Selected" };
        await using var session = theStore.LightweightSession();
        session.Store(roster);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var ids = await session.Query<NullableRoster>()
            .Where(x => x.Id == roster.Id)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ids.Single().ShouldBe(roster.Id);
    }

    [Fact]
    public async Task delete_by_a_nullable_wrapper_id()
    {
        var roster = new NullableRoster { Id = new RosterId(Guid.NewGuid()), Name = "Deleted" };
        await using var session = theStore.LightweightSession();
        session.Store(roster);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Delete<NullableRoster>(roster.Id!.Value.Value);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<NullableRoster>(roster.Id!.Value.Value, TestContext.Current.CancellationToken))
            .ShouldBeNull();
    }
}
