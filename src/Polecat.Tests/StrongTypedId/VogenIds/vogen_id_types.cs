using Vogen;

namespace Polecat.Tests.StrongTypedId.VogenIds;

// Vogen value objects, the second strong-typed-id library Marten's ValueTypeTests covers. Vogen
// emits a *private* constructor plus a static `From` factory, so these exercise the "builder" branch
// of JasperFx's ValueTypeInfo that a hand-written `record struct OrderId(Guid Value)` never reaches.
// Vogen also generates a System.Text.Json converter that writes the inner value, so a Vogen member
// lands in the document JSON as a bare scalar rather than a nested object.
//
// Every Id below is *nullable*, which is Vogen's own required pattern and what Marten's Vogen tests
// use: Vogen prohibits an uninitialized value object, so reading `.Value` off a `default` instance
// throws. A non-nullable Vogen id would throw inside identity assignment before Polecat ever sees
// whether the id was set. StronglyTypedId, which permits `default`, is exercised non-nullably in
// ../GeneratedIds.

[ValueObject<Guid>]
public readonly partial struct VogenInvoiceId;

public class VogenInvoice
{
    public VogenInvoiceId? Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[ValueObject<int>]
public readonly partial struct VogenOrderId;

public class VogenOrder
{
    public VogenOrderId? Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[ValueObject<long>]
public readonly partial struct VogenIssueId;

public class VogenIssue
{
    public VogenIssueId? Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[ValueObject<string>]
public readonly partial struct VogenTeamId;

public class VogenTeam
{
    public VogenTeamId? Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
