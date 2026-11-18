using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_natural_key_aggregate_types

public record OrderNumber(string Value);

public partial class OrderAggregate
{
    public Guid Id { get; set; }

    [NaturalKey]
    public OrderNumber OrderNum { get; set; } = null!;

    public decimal TotalAmount { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public bool IsComplete { get; set; }

    [NaturalKeySource]
    public void Apply(NkOrderCreated e)
    {
        OrderNum = e.OrderNumber;
        CustomerName = e.CustomerName;
    }

    public void Apply(NkOrderItemAdded e)
    {
        TotalAmount += e.Price;
    }

    [NaturalKeySource]
    public void Apply(NkOrderNumberChanged e)
    {
        OrderNum = e.NewOrderNumber;
    }

    public void Apply(NkOrderCompleted e)
    {
        IsComplete = true;
    }
}

public partial class InvoiceAggregate
{
    public Guid Id { get; set; }

    [NaturalKey]
    public string InvoiceCode { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    [NaturalKeySource]
    public void Apply(NkInvoiceCreated e)
    {
        InvoiceCode = e.Code;
        Amount = e.Amount;
    }
}

public record NkOrderCreated(OrderNumber OrderNumber, string CustomerName);
public record NkOrderItemAdded(string ItemName, decimal Price);
public record NkOrderNumberChanged(OrderNumber NewOrderNumber);
public record NkOrderCompleted;
public record NkInvoiceCreated(string Code, decimal Amount);

#endregion
