namespace Polecat.EntityFrameworkCore.Tests;

// Single-stream events
public record OrderPlaced(Guid OrderId, string CustomerName, decimal Amount, int Items);
public record OrderShipped(Guid OrderId);
public record OrderCancelled(Guid OrderId);

// Multi-stream events
public record CustomerOrderPlaced(Guid OrderId, string CustomerName, decimal Amount);
public record CustomerOrderCompleted(Guid OrderId, string CustomerName);

// #489: fan-out event — ONE event names many players, so Identities<T> turns it into many slices
// inside a single tenant group, which is the shape that made AggregationRunner's 10-wide block
// drive one DbContext from ten threads at once.
public record TeamScored(Guid GameId, string[] PlayerNames, int Points);

// EF Core entity written as side effect
public class OrderSummary
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
    public string Status { get; set; } = "Pending";
}

// Single-stream aggregate stored via EF Core
public class Order
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
    public bool IsShipped { get; set; }
    public bool IsCancelled { get; set; }
}

// Multi-stream aggregate stored via EF Core
public class CustomerOrderHistory
{
    public string Id { get; set; } = string.Empty;
    public int TotalOrders { get; set; }
    public decimal TotalSpent { get; set; }
}

// #489: fan-out aggregate. Points is mutated in place on a snapshot the DbContext is tracking,
// which is what a concurrent Entry() runs change detection over.
public class PlayerTally
{
    public string Id { get; set; } = string.Empty;
    public int Points { get; set; }
    public int Appearances { get; set; }
}

// Side effect entity for event projection
public class OrderDetail
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
    public bool IsShipped { get; set; }
    public string Status { get; set; } = "Unknown";
}

// Polecat document stored alongside EF Core entities
public class OrderLog
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
}

// Tenanted aggregate
public class TenantedOrder : Metadata.ITenanted
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
    public bool IsShipped { get; set; }
    public bool IsCancelled { get; set; }
    public string TenantId { get; set; } = string.Empty;
}

// Non-tenanted aggregate (for validation tests)
public class NonTenantedOrder
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
}
