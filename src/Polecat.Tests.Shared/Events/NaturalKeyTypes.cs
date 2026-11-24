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

#region sample_polecat_mapped_natural_key_aggregate

// An async read model of an order stream. It owns no lookup: the order number resolves through
// OrderAggregate's pc_natural_key_orderaggregate table, which OrderAggregate's projection keeps inline.
public partial class NkOrderSummary
{
    public Guid Id { get; set; }

    [MappedNaturalKey<OrderAggregate>]
    public OrderNumber Number { get; set; } = null!;

    public int Items { get; set; }

    public static NkOrderSummary Create(NkOrderCreated e) => new() { Number = e.OrderNumber };

    public void Apply(NkOrderItemAdded e) => Items++;

    public void Apply(NkOrderNumberChanged e) => Number = e.NewOrderNumber;
}

#endregion

#region sample_polecat_nullable_natural_key_aggregate

public readonly record struct NkSku(string Value);

public record NkSkuListed(NkSku Sku, string Name);

public record NkSkuChanged(NkSku? NewSku);

public record NkSkuRestocked(int Quantity);

// The key is a struct, declared nullable on the aggregate, and set by sources returning both NkSku and NkSku?
public partial class NkSkuProduct
{
    public Guid Id { get; set; }

    [NaturalKey]
    public NkSku? Sku { get; set; }

    public string Name { get; set; } = string.Empty;
    public int Stock { get; set; }

    [NaturalKeySource]
    public static NkSku KeyFor(NkSkuListed e) => e.Sku;

    [NaturalKeySource]
    public static NkSku? KeyFor(NkSkuChanged e) => e.NewSku;

    public void Apply(NkSkuListed e)
    {
        Sku = e.Sku;
        Name = e.Name;
    }

    public void Apply(NkSkuChanged e) => Sku = e.NewSku;

    public void Apply(NkSkuRestocked e) => Stock += e.Quantity;
}

#endregion

// An async view mapped to the owner's key, declared non-nullable where the owner's is nullable
public partial class NkSkuStock
{
    public Guid Id { get; set; }

    [MappedNaturalKey<NkSkuProduct>]
    public NkSku Sku { get; set; }

    public int Stock { get; set; }

    public static NkSkuStock Create(NkSkuListed e) => new() { Sku = e.Sku };

    public void Apply(NkSkuRestocked e) => Stock += e.Quantity;
}

