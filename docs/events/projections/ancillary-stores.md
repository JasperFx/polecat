# Using Ancillary Stores in Projections

## The Problem

When building systems with multiple Polecat stores (using `AddPolecatStore<T>()`), it's common to
need projections in one store that reference data from another. For example, a billing projection
in your primary store might need to look up tariff data from a separate `ITarievenStore`.

Directly injecting an ancillary store via constructor can cause startup deadlocks because the
DI container may attempt to resolve the ancillary store while the primary store is still being
constructed.

## Solution: Inject `Lazy<T>`

`AddPolecatStore<T>()` automatically registers `Lazy<T>` in the DI container alongside the
store itself. This lets you inject a lazy reference that defers resolution until the store is
actually needed — safely past the startup phase:

```csharp
public interface ITarievenStore : IDocumentStore;

public class InvoiceProjection : SingleStreamProjection<Invoice, Guid>
{
    private readonly Lazy<ITarievenStore> _tarievenStore;

    public InvoiceProjection(Lazy<ITarievenStore> tarievenStore)
    {
        _tarievenStore = tarievenStore;
    }

    public override async Task EnrichEventsAsync(
        SliceGroup<Invoice, Guid> group,
        IQuerySession querySession,
        CancellationToken cancellation)
    {
        // Safe - the store is fully constructed by the time
        // EnrichEventsAsync runs
        await using var session = _tarievenStore.Value.QuerySession();

        var ids = group.Slices
            .SelectMany(s => s.Events().OfType<IEvent<ServicePerformed>>())
            .Select(e => e.Data.TariefId)
            .Distinct().ToArray();

        var tarieven = await session.LoadManyAsync<Tarief>(cancellation, ids);

        foreach (var slice in group.Slices)
        {
            foreach (var e in slice.Events().OfType<IEvent<ServicePerformed>>())
            {
                if (tarieven.TryGetValue(e.Data.TariefId, out var tarief))
                {
                    e.Data.ResolvedPrice = tarief.Price;
                }
            }
        }
    }
}
```

Register the stores and projection:

```csharp
services.AddPolecat(opts =>
{
    opts.ConnectionString = "primary connection string";
});

services.AddPolecatStore<ITarievenStore>(opts =>
{
    opts.ConnectionString = "tarieven connection string";
});
```

### Why `Lazy<T>` Works

The `Lazy<T>` wrapper is constructed immediately (it's just a thin wrapper), but the inner
`IDocumentStore` isn't resolved until `.Value` is accessed. By the time your projection's
methods execute, all stores are fully constructed and the lazy resolution succeeds without
deadlock.

### Multiple Ancillary Stores

You can inject multiple lazy store references. Each `AddPolecatStore<T>()` call automatically
registers its own `Lazy<T>`:

```csharp
public class CrossStoreProjection : SingleStreamProjection<Summary, Guid>
{
    private readonly Lazy<ITarievenStore> _tarieven;
    private readonly Lazy<IDebtorsStore> _debtors;

    public CrossStoreProjection(
        Lazy<ITarievenStore> tarieven,
        Lazy<IDebtorsStore> debtors)
    {
        _tarieven = tarieven;
        _debtors = debtors;
    }

    // Use _tarieven.Value and _debtors.Value in your projection methods
}
```
