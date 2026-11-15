using System.Text.Json.Serialization;

namespace Polecat.Specs;

// #808: the C# 15 unions the specs append and query. System.Text.Json writes a union as its bare active
// case; reading one back needs a classifier once two cases are JSON objects, and the structural one works
// here because Card's and Cash's properties are disjoint.

public record Card(string Last4);

public record Cash(decimal Amount);

[JsonUnion(TypeClassifier = typeof(JsonUnionTypeStructuralClassifier))]
public union Payment(Card, Cash);

public class Order
{
    public Guid Id { get; set; }
    public string Customer { get; set; } = string.Empty;
    public Payment Payment { get; set; }
}

public record AccountOpened(string Owner);

public record AccountCredited(decimal Amount);

public record AccountDebited(decimal Amount, string Reason);

public union AccountActivity(AccountCredited, AccountDebited);

/// <summary>Self-aggregating, with one Apply for the whole union (jasperfx#1053).</summary>
public partial class Account
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = string.Empty;
    public decimal Balance { get; set; }
    public int Debits { get; set; }

    public static Account Create(AccountOpened e) => new() { Owner = e.Owner };

    public void Apply(AccountActivity e)
    {
        switch (e)
        {
            case AccountCredited credited:
                Balance += credited.Amount;
                break;
            case AccountDebited debited:
                Balance -= debited.Amount;
                Debits++;
                break;
        }
    }
}
