using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;
using StronglyTypedIds;
using Vogen;

namespace Polecat.Tests.StrongTypedId;

// Mirrors Marten's ValueTypeTests duplicated_value_type_field_operations, Bugs/querying_by_value_types
// and Bug_4288: a strong-typed value used somewhere *other* than the Id. Both generators emit a
// System.Text.Json converter that writes the inner value, so the member lands in the document JSON as
// a bare scalar and is queryable against its inner type — but the CLR value in the predicate is still
// the wrapper, which SqlClient has no mapping for. These pin the unwrapping.
//
// Marten reaches the same place with `Duplicate(x => x.Member)`, which projects the value into its own
// column. Polecat has no duplicated columns — it queries the JSON path directly — so there is nothing
// to configure here.

[ValueObject<Guid>]
public readonly partial struct TeacherRef;

[ValueObject<string>]
public readonly partial struct EmailAddress;

[ValueObject<int>]
public partial record Age;

[StronglyTypedId(Template.Guid)]
public readonly partial struct GeneratedTeacherRef;

/// <summary>Marten's Bug_4288 shape: a real factory shadowed by a nullable sibling.</summary>
[ValueObject<string>]
public readonly partial struct SiblingFactoryValue
{
    public static SiblingFactoryValue? FromNullable(string? value)
        => value is null ? null : From(value);
}

public class ClassRoom
{
    public Guid Id { get; set; }
    public TeacherRef Teacher { get; set; }
    public GeneratedTeacherRef Substitute { get; set; }
    public string Subject { get; set; } = string.Empty;
}

public class Customer
{
    public Guid Id { get; set; }
    public EmailAddress Email { get; set; }
    public Age Age { get; set; } = Age.From(0);
}

public class SiblingFactoryDoc
{
    public Guid Id { get; set; }
    public SiblingFactoryValue Value { get; set; }
}

[Collection("integration")]
public class value_type_member_operations : IntegrationContext
{
    public value_type_member_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "value_type_member"; });
    }

    private static ClassRoom Room(string subject) => new()
    {
        Id = Guid.NewGuid(),
        Teacher = TeacherRef.From(Guid.NewGuid()),
        Substitute = new GeneratedTeacherRef(Guid.NewGuid()),
        Subject = subject
    };

    [Fact]
    public async Task store_a_document_smoke_test()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Room("Smoke"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await session.Query<ClassRoom>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task load_document_round_trips_the_member()
    {
        var room = Room("Round Trip");
        await using var session = theStore.LightweightSession();
        session.Store(room);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<ClassRoom>(room.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Teacher.ShouldBe(room.Teacher);
        loaded.Substitute.ShouldBe(room.Substitute);
    }

    [Fact]
    public async Task the_member_serializes_as_a_bare_scalar()
    {
        // Not a cosmetic assertion: if the generator's JSON converter were absent, the member would
        // serialize as { "value": ... } and every predicate below would silently match nothing.
        var room = Room("Json");
        await using var session = theStore.LightweightSession();
        session.Store(room);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var json = await session.Query<ClassRoom>()
            .Where(x => x.Id == room.Id)
            .ToJsonArrayAsync(TestContext.Current.CancellationToken);

        json.ShouldContain($"\"teacher\":\"{room.Teacher.Value}\"");
        json.ShouldContain($"\"substitute\":\"{room.Substitute.Value}\"");
    }

    [Fact]
    public async Task use_in_LINQ_where_clause()
    {
        var room = Room("Where");
        await using var session = theStore.LightweightSession();
        session.Store(room);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.Query<ClassRoom>()
            .Where(x => x.Teacher == room.Teacher)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded!.Subject.ShouldBe("Where");
    }

    [Fact]
    public async Task use_a_generated_id_member_in_LINQ_where_clause()
    {
        var room = Room("Generated Where");
        await using var session = theStore.LightweightSession();
        session.Store(room);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.Query<ClassRoom>()
            .Where(x => x.Substitute == room.Substitute)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded!.Subject.ShouldBe("Generated Where");
    }

    [Fact]
    public async Task use_in_LINQ_order_clause()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Room("Order A"), Room("Order B"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<ClassRoom>()
            .OrderBy(x => x.Teacher)
            .Take(3)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task use_in_LINQ_select_clause()
    {
        var room = Room("Select");
        await using var session = theStore.LightweightSession();
        session.Store(room);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var teachers = await session.Query<ClassRoom>()
            .Where(x => x.Id == room.Id)
            .Select(x => x.Teacher)
            .ToListAsync(TestContext.Current.CancellationToken);

        teachers.Single().ShouldBe(room.Teacher);
    }

    [Fact]
    public async Task use_in_LINQ_is_one_of()
    {
        var one = Room("One");
        var two = Room("Two");

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var results = await session.Query<ClassRoom>()
            .Where(x => x.Teacher.IsOneOf(one.Teacher, two.Teacher))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task collect_the_referenced_ids_for_a_manual_include()
    {
        // Polecat has no Include() operator (Marten's include_usage covers that), but the piece an
        // Include depends on — projecting a wrapper-typed reference out of the matching documents and
        // loading the referenced documents by it — works.
        var one = Room("Include One");
        var two = Room("Include Two");

        await using var session = theStore.LightweightSession();
        session.Store(one, two);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var teacherIds = await session.Query<ClassRoom>()
            .Where(x => x.Id.IsOneOf(one.Id, two.Id))
            .Select(x => x.Teacher)
            .ToListAsync(TestContext.Current.CancellationToken);

        teacherIds.Count.ShouldBe(2);
        teacherIds.ShouldContain(one.Teacher);
        teacherIds.ShouldContain(two.Teacher);
    }

    [Fact]
    public async Task query_by_a_string_and_a_reference_type_value_object()
    {
        // Marten's Bugs/querying_by_value_types. `Age` is a Vogen *record* — a reference-type value
        // object — so this also covers the non-struct wrapper shape.
        var customer = new Customer
        {
            Id = Guid.NewGuid(),
            Email = EmailAddress.From("example@me.com"),
            Age = Age.From(25)
        };

        await using var session = theStore.LightweightSession();
        session.Store(customer);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await session.LoadAsync<Customer>(customer.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded!.Email.Value.ShouldBe("example@me.com");

        var byEmail = await session.Query<Customer>()
            .Where(x => x.Email == customer.Email)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);
        byEmail.ShouldNotBeNull();

        var byAge = await session.Query<Customer>()
            .Where(x => x.Age == customer.Age)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);
        byAge.ShouldNotBeNull();
    }

    [Fact]
    public async Task round_trip_and_query_a_value_type_with_a_nullable_sibling_factory()
    {
        // marten#4288. Without preferring the factory whose return type is the value type itself,
        // building the wrapper throws "Expression of type 'Nullable<T>' cannot be used for return
        // type 'T'" the first time this member is resolved.
        var doc = new SiblingFactoryDoc { Id = Guid.NewGuid(), Value = SiblingFactoryValue.From("abc") };

        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var key = SiblingFactoryValue.From("abc");
        var found = await session.Query<SiblingFactoryDoc>()
            .Where(x => x.Value == key)
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Count.ShouldBe(1);
        found.Single().Id.ShouldBe(doc.Id);
    }
}
