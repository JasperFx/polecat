using System.Text.Json.Serialization;

namespace Polecat.Tests.Unions;

// #808: the C# 15 unions the union tests query and append. net11.0 only (see Polecat.Tests.csproj).
//
// System.Text.Json writes a union as its bare active case (jasperfx#1037). Reading one back takes a
// classifier as soon as two cases are JSON objects, and the structural classifier refuses a union where
// one case's property names contain another's even on WRITE. So UnionPayment, whose object cases are
// disjoint, carries the structural classifier and round-trips; UnionLedgerEntry, the typical event-shaped
// union, can be stored but never read back as a union, and its tests only count or project ids.

public record UnionCard(string Last4);

public record UnionCash(decimal Amount);

[JsonUnion(TypeClassifier = typeof(JsonUnionTypeStructuralClassifier))]
public union UnionPayment(UnionCard, UnionCash);

/// <summary>Primitive cases, told apart by JSON kind; STJ reads these back with no classifier.</summary>
public union UnionCode(int, string);

public record UnionDeposited(decimal Amount);

public record UnionWithdrawn(decimal Amount, string Reason);

/// <summary>One case's shape contains the other's: Deposited is recognised by Reason being absent.</summary>
public union UnionLedgerEntry(UnionDeposited, UnionWithdrawn);

public record UnionMeters(double Value);

public record UnionFeet(double Value);

/// <summary>Two cases with identical shapes: no stored value says which one it is.</summary>
public union UnionLength(UnionMeters, UnionFeet);

/// <summary>Two cases written as a JSON number.</summary>
public union UnionNumber(int, double);

public record UnionTagged(string? Note);

public record UnionPlain();

/// <summary>Tagged's only property is nullable, and a null is left out, so a Tagged can look like a Plain.</summary>
public union UnionMaybeTagged(UnionTagged, UnionPlain);

/// <summary>A case that is itself a union.</summary>
public union UnionNested(UnionPayment, string);

public class UnionHolder
{
    public UnionPayment Payment { get; set; }
}

public class UnionOrder
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public UnionPayment Payment { get; set; }
    public UnionCode Code { get; set; }
    public UnionPayment? Maybe { get; set; }
    public UnionHolder? Holder { get; set; }
}

public class UnionLedgerDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public UnionLedgerEntry Entry { get; set; }
}

public class UnionRefusalDoc
{
    public Guid Id { get; set; }
    public UnionLength Length { get; set; }
    public UnionNumber Number { get; set; }
    public UnionMaybeTagged Tag { get; set; }
    public UnionNested Nested { get; set; }
    public object? Anything { get; set; }
}

// Events

public record UnionAccountOpened(string Owner);

public record UnionAccountCredited(decimal Amount);

public record UnionAccountDebited(decimal Amount, string Reason);

public union UnionAccountEvent(UnionAccountCredited, UnionAccountDebited);

/// <summary>Self-aggregating, with one Apply for the whole union (jasperfx#1053).</summary>
public partial class UnionAccount
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = string.Empty;
    public decimal Balance { get; set; }
    public int Debits { get; set; }

    public static UnionAccount Create(UnionAccountOpened e) => new() { Owner = e.Owner };

    public void Apply(UnionAccountEvent e)
    {
        switch (e)
        {
            case UnionAccountCredited credited:
                Balance += credited.Amount;
                break;
            case UnionAccountDebited debited:
                Balance -= debited.Amount;
                Debits++;
                break;
        }
    }
}
