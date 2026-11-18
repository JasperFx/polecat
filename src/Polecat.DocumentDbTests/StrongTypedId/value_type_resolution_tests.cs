using Polecat.Storage;
using Shouldly;
using StronglyTypedIds;
using Vogen;

namespace Polecat.Tests.StrongTypedId;

// Mirrors Marten's ValueTypeTests/applicability_of_identity_types + registration: which CLR types
// Polecat accepts as a strong-typed-id wrapper, which member it treats as the inner value, and
// whether it builds the wrapper through a constructor or a static factory. These are pure resolution
// tests — no database — so they pin the rule itself rather than one document type's behavior.

[ValueObject<Guid>]
public readonly partial struct ResolvableGuidId;

[ValueObject<int>]
public readonly partial struct ResolvableIntId;

[ValueObject<int>]
public readonly partial struct WeirdNamed;

[ValueObject<long>]
public readonly partial struct ResolvableLongId;

[ValueObject<string>]
public readonly partial struct ResolvableStringId;

[ValueObject<DateOnly>]
public readonly partial struct ResolvableDateId;

[StronglyTypedId(Template.Guid)]
public readonly partial struct GeneratedResolvableId;

public record struct NewGuidId(Guid Value);

public record struct NewIntId(int Value);

public record struct NewLongId(long Value);

public record struct NewStringId(string Value);

public record struct NewDateId(DateOnly Value);

/// <summary>
///     The shape Marten documents as valid: a public property getter for the inner value paired with
///     a public static factory taking the inner value.
/// </summary>
public readonly struct SpecialValue
{
    private SpecialValue(string value) => Value = value;

    public string Value { get; }

    public static SpecialValue From(string value) => new(value);
}

/// <summary>
///     The same, but with a nullable sibling factory alongside the real one — marten#4288. Picking
///     the sibling yields a builder that returns <c>SpecialValueWithNullableSibling?</c>, which blows
///     up when the wrapper delegate is compiled.
/// </summary>
public readonly struct SpecialValueWithNullableSibling
{
    private SpecialValueWithNullableSibling(string value) => Value = value;

    public string Value { get; }

    public static SpecialValueWithNullableSibling? FromNullable(string? value)
        => value is null ? null : From(value);

    public static SpecialValueWithNullableSibling From(string value) => new(value);
}

/// <summary>A struct with two public properties — no single inner value to wrap.</summary>
public readonly struct NotValidPair
{
    public string First { get; init; }
    public string Second { get; init; }
}

/// <summary>A struct with one property but no way to build it from that value.</summary>
public readonly struct NoBuilder
{
    public string Value { get; init; }
}

public class DefinitelyNotValid;

public class Document<T>
{
    public T Id { get; set; } = default!;
}

public class value_type_resolution_tests
{
    [Theory]
    // Plain scalars are ids in their own right, never wrappers.
    [InlineData(typeof(int), false)]
    [InlineData(typeof(string), false)]
    [InlineData(typeof(Guid), false)]
    // Vogen value objects over each supported inner type.
    [InlineData(typeof(ResolvableGuidId), true)]
    [InlineData(typeof(ResolvableIntId), true)]
    [InlineData(typeof(WeirdNamed), true)]
    [InlineData(typeof(ResolvableLongId), true)]
    [InlineData(typeof(ResolvableStringId), true)]
    // ... and their nullable forms, which resolve to the same wrapper.
    [InlineData(typeof(ResolvableGuidId?), true)]
    [InlineData(typeof(ResolvableIntId?), true)]
    [InlineData(typeof(ResolvableLongId?), true)]
    [InlineData(typeof(ResolvableStringId?), true)]
    // A wrapper over an inner type Polecat cannot store as an id is not a candidate.
    [InlineData(typeof(ResolvableDateId), false)]
    [InlineData(typeof(ResolvableDateId?), false)]
    // The StronglyTypedId generator's output.
    [InlineData(typeof(GeneratedResolvableId), true)]
    // Hand-written record structs.
    [InlineData(typeof(NewGuidId), true)]
    [InlineData(typeof(NewIntId), true)]
    [InlineData(typeof(NewLongId), true)]
    [InlineData(typeof(NewStringId), true)]
    [InlineData(typeof(NewDateId), false)]
    // Shapes that are not single-value wrappers at all.
    [InlineData(typeof(NotValidPair), false)]
    [InlineData(typeof(NoBuilder), false)]
    [InlineData(typeof(DefinitelyNotValid), false)]
    public void is_candidate_value_type(Type candidate, bool isCandidate)
    {
        (ValueTypes.TryResolve(candidate) != null).ShouldBe(isCandidate);
    }

    [Theory]
    [InlineData(typeof(ResolvableGuidId), typeof(Guid))]
    [InlineData(typeof(ResolvableIntId), typeof(int))]
    [InlineData(typeof(ResolvableLongId), typeof(long))]
    [InlineData(typeof(ResolvableStringId), typeof(string))]
    [InlineData(typeof(ResolvableGuidId?), typeof(Guid))]
    [InlineData(typeof(GeneratedResolvableId), typeof(Guid))]
    [InlineData(typeof(NewGuidId), typeof(Guid))]
    [InlineData(typeof(NewIntId), typeof(int))]
    [InlineData(typeof(NewLongId), typeof(long))]
    [InlineData(typeof(NewStringId), typeof(string))]
    public void resolves_the_inner_type(Type wrapper, Type expectedInner)
    {
        ValueTypes.TryResolve(wrapper)!.SimpleType.ShouldBe(expectedInner);
    }

    [Fact]
    public void record_struct_resolves_through_its_constructor()
    {
        var info = ValueTypes.TryResolve(typeof(NewGuidId))!;
        info.Ctor.ShouldNotBeNull();
        info.ValueProperty.Name.ShouldBe("Value");
    }

    [Fact]
    public void private_ctor_type_resolves_through_its_static_factory()
    {
        var info = ValueTypes.TryResolve(typeof(SpecialValue))!;
        info.Ctor.ShouldBeNull();
        info.Builder!.Name.ShouldBe("From");
        info.ValueProperty.Name.ShouldBe("Value");
    }

    [Fact]
    public void picks_the_builder_whose_return_type_matches_the_value_type()
    {
        // marten#4288: with FromNullable(string?) alongside From(string), the naive "first static
        // method taking one string" pick returns Nullable<T> and every wrapper build fails.
        var info = ValueTypes.TryResolve(typeof(SpecialValueWithNullableSibling))!;
        info.Builder!.Name.ShouldBe("From");
        info.Builder.ReturnType.ShouldBe(typeof(SpecialValueWithNullableSibling));
    }

    [Theory]
    [InlineData(typeof(ResolvableGuidId), typeof(Guid))]
    [InlineData(typeof(ResolvableIntId), typeof(int))]
    [InlineData(typeof(ResolvableLongId), typeof(long))]
    [InlineData(typeof(ResolvableStringId), typeof(string))]
    [InlineData(typeof(ResolvableGuidId?), typeof(Guid))]
    [InlineData(typeof(GeneratedResolvableId), typeof(Guid))]
    [InlineData(typeof(NewGuidId), typeof(Guid))]
    public void document_mapping_maps_the_id_to_the_inner_type(Type idType, Type expectedInner)
    {
        var mapping = new DocumentMapping(typeof(Document<>).MakeGenericType(idType), new StoreOptions());

        mapping.IsStrongTypedId.ShouldBeTrue();
        mapping.IdType.ShouldBe(idType);
        mapping.InnerIdType.ShouldBe(expectedInner);
    }

    [Theory]
    [InlineData(typeof(Guid))]
    [InlineData(typeof(int))]
    [InlineData(typeof(long))]
    [InlineData(typeof(string))]
    public void document_mapping_leaves_plain_ids_alone(Type idType)
    {
        var mapping = new DocumentMapping(typeof(Document<>).MakeGenericType(idType), new StoreOptions());

        mapping.IsStrongTypedId.ShouldBeFalse();
        mapping.InnerIdType.ShouldBe(idType);
    }

    [Theory]
    [InlineData(typeof(ResolvableDateId))]
    [InlineData(typeof(NewDateId))]
    [InlineData(typeof(NotValidPair))]
    [InlineData(typeof(DefinitelyNotValid))]
    public void document_mapping_rejects_an_unusable_id_type(Type idType)
    {
        Should.Throw<InvalidOperationException>(() =>
            new DocumentMapping(typeof(Document<>).MakeGenericType(idType), new StoreOptions()));
    }
}
