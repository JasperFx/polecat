using StronglyTypedIds;

namespace Polecat.Tests.StrongTypedId.GeneratedIds;

// The StronglyTypedId generator, which Marten's ValueTypeTests/StrongTypedId suites use alongside
// hand-written record structs. It emits a readonly record struct with a *public* constructor, so
// these cover the "ctor" branch of JasperFx's ValueTypeInfo the way ../VogenIds covers the static
// factory branch — and, unlike Vogen, it permits `default`, so the ids here are non-nullable and
// exercise identity assignment against a zero-valued wrapper.
//
// Polecat already used this generator for aggregate identities (see ../../Projections); these are
// the document-side equivalents.

[StronglyTypedId(Template.Guid)]
public readonly partial struct GeneratedInvoiceId;

public class GeneratedInvoice
{
    public GeneratedInvoiceId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[StronglyTypedId(Template.Int)]
public readonly partial struct GeneratedOrderId;

public class GeneratedOrder
{
    public GeneratedOrderId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[StronglyTypedId(Template.Long)]
public readonly partial struct GeneratedIssueId;

public class GeneratedIssue
{
    public GeneratedIssueId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[StronglyTypedId(Template.String)]
public readonly partial struct GeneratedTeamId;

public class GeneratedTeam
{
    public GeneratedTeamId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
