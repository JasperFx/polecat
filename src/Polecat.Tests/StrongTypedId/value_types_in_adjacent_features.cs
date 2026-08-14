using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Projections.Flattened;
using Polecat.Storage;
using Polecat.TestUtils;
using Polecat.Tests.Harness;
using Shouldly;
using Vogen;

namespace Polecat.Tests.StrongTypedId;

// Surfaces beyond document Load/Query that also have to unwrap a strong-typed value, plus the
// regression that keeps the unwrapping from being applied too eagerly. Ported from the Marten tests
// that live outside ValueTypeTests: LinqTests/Bugs/Bug_money_value_object_misdetected_as_strong_typed_id
// and EventSourcingTests/Projections/Flattened/Bug_4290_4291_flat_table_enum_and_value_types.

[ValueObject<Guid>]
public readonly partial struct AccountRef;

[ValueObject<int>]
public readonly partial struct LineCount;

public record LedgerPosted(Guid Id, AccountRef Account, LineCount Lines);

public class LedgerFlatProjection : FlatTableProjection
{
    public LedgerFlatProjection() : base("value_type_ledger", "value_type_flat")
    {
        Table.AddColumn("id", "uniqueidentifier").AsPrimaryKey();

        Project<LedgerPosted>(map =>
        {
            map.Map(x => x.Account, "account");
            map.Map(x => x.Lines, "lines");
        });
    }
}

/// <summary>
///     A multi-property record struct — a value object, but not a strong-typed *id*. It must stay a
///     nested JSON object; treating it as a scalar wrapper breaks any nested member access.
/// </summary>
public readonly record struct Money(decimal Value, Guid CurrencyId)
{
    public static Money Zero(Guid currencyId) => new(0m, currencyId);
}

public class MoneyDoc
{
    public Guid Id { get; set; }
    public Money Amount { get; set; }
}

public class IndexedRoom
{
    public Guid Id { get; set; }
    public AccountRef Account { get; set; }
}

[Collection("integration")]
public class value_types_in_adjacent_features : IntegrationContext
{
    public value_types_in_adjacent_features(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public void multi_property_record_struct_is_not_a_strong_typed_id()
    {
        // Money has two public properties, so there is no single inner value to wrap. If it were
        // mis-detected, its column would be typed from one of them and `x.Amount.Value` would stop
        // resolving as a nested JSON path.
        ValueTypes.TryResolve(typeof(Money)).ShouldBeNull();
        ValueTypes.TryResolve(typeof(Money), allowReferenceTypes: true).ShouldBeNull();
    }

    [Fact]
    public async Task linq_resolves_nested_member_access_on_a_multi_property_record_struct()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "value_type_money"; });

        var doc = new MoneyDoc { Id = Guid.NewGuid(), Amount = new Money(12.5m, Guid.NewGuid()) };

        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var found = await session.Query<MoneyDoc>()
            .Where(x => x.Amount.Value > 0m && x.Amount.CurrencyId == doc.Amount.CurrencyId)
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Count.ShouldBe(1);
        found.Single().Id.ShouldBe(doc.Id);
    }

    [Fact]
    public async Task index_over_a_value_type_member_gets_the_inner_column_type()
    {
        // Marten's Bug_4288 index-DDL case. The computed column and the LINQ predicate are both built
        // from the resolved member type, so if the wrapper is not unwrapped here the column lands as
        // varchar(250), stops matching the uniqueidentifier-typed predicate, and the index is dead.
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "value_type_index";
            opts.Schema.For<IndexedRoom>().Index(x => x.Account);
        });

        await using var session = theStore.LightweightSession();
        var room = new IndexedRoom { Id = Guid.NewGuid(), Account = AccountRef.From(Guid.NewGuid()) };
        session.Store(room);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_indexedroom", "value_type_index");
        columns.Single(c => c.Name.Contains("account", StringComparison.OrdinalIgnoreCase))
            .TypeName.ShouldBe("uniqueidentifier");

        var found = await session.Query<IndexedRoom>()
            .Where(x => x.Account == room.Account)
            .ToListAsync(TestContext.Current.CancellationToken);
        found.Single().Id.ShouldBe(room.Id);
    }

    [Fact]
    public async Task flat_table_projection_maps_value_types_to_their_inner_columns()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "value_type_flat";
            opts.Projections.Add<LedgerFlatProjection>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        var account = AccountRef.From(Guid.NewGuid());

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new LedgerPosted(streamId, account, LineCount.From(7)));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var columns = await SchemaInspector.GetColumnInfoAsync("value_type_ledger", "value_type_flat");
        columns.Single(c => c.Name == "account").TypeName.ShouldBe("uniqueidentifier");
        columns.Single(c => c.Name == "lines").TypeName.ShouldBe("int");

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select account, lines from value_type_flat.value_type_ledger where id = @id";
        cmd.Parameters.AddWithValue("@id", streamId);

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        (await reader.ReadAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
        reader.GetGuid(0).ShouldBe(account.Value);
        reader.GetInt32(1).ShouldBe(7);
    }
}
