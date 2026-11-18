using JasperFx.Core.Reflection;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;
using Vogen;

namespace Polecat.Tests.StrongTypedId;

// polecat#459. Mirrors Marten's ValueTypeTests/registration.cs. Polecat resolves value types on its
// own, so nothing here is *required* — the API exists so that one shared store-configuration file can
// compile against Marten and Polecat both. These tests pin the contract that portability depends on:
// the same call, the same return type, and the same loud failure on a type that cannot be a wrapper.

[ValueObject<Guid>]
public readonly partial struct RegisteredAlertId;

public record struct RegisteredExternalId(string Value);

/// <summary>Private ctor plus a static factory — the shape Vogen and hand-rolled wrappers share.</summary>
public readonly struct RegisteredSpecialValue
{
    private RegisteredSpecialValue(string value) => Value = value;

    public string Value { get; }

    public static RegisteredSpecialValue From(string value) => new(value);
}

public class RegisteredAlert
{
    public RegisteredAlertId? Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class register_value_type_tests
{
    [Fact]
    public void register_happy_path_with_a_constructor()
    {
        var value = new StoreOptions().RegisterValueType(typeof(RegisteredExternalId));

        value.Ctor.ShouldNotBeNull();
        value.ValueProperty.Name.ShouldBe("Value");
        value.SimpleType.ShouldBe(typeof(string));
    }

    [Fact]
    public void register_happy_path_with_a_static_factory()
    {
        var value = new StoreOptions().RegisterValueType(typeof(RegisteredSpecialValue));

        value.Builder!.Name.ShouldBe("From");
        value.ValueProperty.Name.ShouldBe("Value");
        value.SimpleType.ShouldBe(typeof(string));
    }

    [Fact]
    public void register_the_generic_overload()
    {
        var value = new StoreOptions().RegisterValueType<RegisteredAlertId>();

        value.OuterType.ShouldBe(typeof(RegisteredAlertId));
        value.SimpleType.ShouldBe(typeof(Guid));
    }

    [Fact]
    public void registering_twice_returns_the_same_resolution()
    {
        var options = new StoreOptions();

        var first = options.RegisterValueType<RegisteredAlertId>();
        var second = options.RegisterValueType<RegisteredAlertId>();

        // Resolution is process-wide and cached, so a second call — from another store, or the same
        // shared configuration file run twice — must not produce a second, competing answer.
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public void registering_on_one_store_options_does_not_disturb_another()
    {
        new StoreOptions().RegisterValueType<RegisteredAlertId>();

        var other = new StoreOptions().RegisterValueType<RegisteredAlertId>();

        other.SimpleType.ShouldBe(typeof(Guid));
    }

    [Theory]
    // Two public properties — no single inner value to wrap.
    [InlineData(typeof(NotValidPair))]
    // One property, but nothing that can build it from that value.
    [InlineData(typeof(NoBuilder))]
    // Not a wrapper at all.
    [InlineData(typeof(DefinitelyNotValid))]
    // A wrapper over an inner type Polecat cannot store.
    [InlineData(typeof(ResolvableDateId))]
    public void sad_path_registration_throws(Type type)
    {
        var options = new StoreOptions();

        Should.Throw<InvalidValueTypeException>(() => options.RegisterValueType(type));
    }

    [Fact]
    public void the_sad_path_message_names_the_type_and_says_what_is_required()
    {
        var ex = Should.Throw<InvalidValueTypeException>(
            () => new StoreOptions().RegisterValueType(typeof(NotValidPair)));

        ex.Message.ShouldContain(nameof(NotValidPair));
        ex.Message.ShouldContain("Guid");
    }
}

// The registered type still has to actually work as an identity afterwards — registration must not be
// the only thing that makes a value type usable, since Polecat users will never call it.
[Collection("integration")]
public class registered_value_type_still_round_trips : IntegrationContext
{
    public registered_value_type_still_round_trips(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "registered_value_type";

            // The portable line: identical source under Marten and Polecat.
            opts.RegisterValueType<RegisteredAlertId>();
        });
    }

    [Fact]
    public async Task store_load_and_query_after_explicit_registration()
    {
        var alert = new RegisteredAlert { Name = "Registered" };

        await using var session = theStore.LightweightSession();
        session.Store(alert);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        alert.Id.ShouldNotBeNull();

        var loaded = await session.LoadAsync<RegisteredAlert>(
            alert.Id!.Value.Value, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Id.ShouldBe(alert.Id);

        var queried = await session.Query<RegisteredAlert>()
            .Where(x => x.Id == alert.Id)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);
        queried!.Name.ShouldBe("Registered");
    }
}
