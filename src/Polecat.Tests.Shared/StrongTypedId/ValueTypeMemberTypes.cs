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
