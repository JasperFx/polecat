# Migration Guide

## Key Changes in 4.0.0

Polecat 4.0 ships in lockstep with [Marten 9.0](https://martendb.io) and [JasperFx 2.0](https://github.com/JasperFx/jasperfx/issues/215) as part of the [Critter Stack 2026](https://github.com/JasperFx/jasperfx/issues/217) release wave. The 4.0 line builds on the same JasperFx 2.0 / JasperFx.Events 2.0 / Weasel 9.0 alphas that Marten 9 consumes — this is the consolidation cycle, not a rewrite. Most upgrades are pin bumps; the breaking surface is small and localized.

### Foundation pin bumps

Polecat 4 consumes the 2026-wave alpha line for the shared substrate. Update your `Directory.Packages.props` (or the equivalent in your csproj):

| Package | 3.x | 4.0 (current alpha) |
|---|---|---|
| `JasperFx` | `1.31.0` | `2.0.0-alpha.11` |
| `JasperFx.Events` | `1.36.0` | `2.0.0-alpha.4` |
| `JasperFx.Events.SourceGenerator` | `1.4.0` | `2.0.0-alpha.2` |
| `Weasel.SqlServer` | `8.15.2` | `9.0.0-alpha.3` |
| `Weasel.EntityFrameworkCore` | `8.15.2` | `9.0.0-alpha.3` |

The alpha line is still rolling forward as the 2026 wave converges; the table above reflects the current Polecat 4 pins. Expect another tick or two before 4.0 GA — keep one set of bumps in your renovate/dependabot config and pin all five packages together.

Target frameworks (`net9.0;net10.0`) are unchanged from late Polecat 3.x; .NET 8 was already dropped before Polecat 3.2.

### Dedup audit relocations

The Marten ↔ Polecat dedup audit ([JasperFx/jasperfx#218](https://github.com/JasperFx/jasperfx/issues/218)) consolidated several enums that had parallel definitions in Polecat and Marten into the canonical Weasel / JasperFx homes. The relocations are mechanical — same values, different namespace.

#### `Polecat.BulkInsertMode` → `Weasel.Core.BulkInsertMode`

[#50](https://github.com/JasperFx/polecat/pull/50), audit row [weasel#264](https://github.com/JasperFx/weasel/issues/264).

Third-party consumers that referenced `Polecat.BulkInsertMode` by full name need to update their `using` directive:

```csharp
// before (Polecat 3.x)
using Polecat;
await store.Advanced.BulkInsertAsync(docs, BulkInsertMode.OverwriteExisting);

// after (Polecat 4.0)
using Weasel.Core;
await store.Advanced.BulkInsertAsync(docs, BulkInsertMode.OverwriteExisting);
```

The enum gained a fourth value — `OverwriteIfVersionMatches` — that did not exist on the Polecat 3.x enum. See the new-behavior section below.

#### `Polecat.Storage.CascadeAction` → `Weasel.Core.CascadeAction`

[#47](https://github.com/JasperFx/polecat/issues/47) / [#61](https://github.com/JasperFx/polecat/pull/61), audit row 2 in [JasperFx/jasperfx#218](https://github.com/JasperFx/jasperfx/issues/218).

`DocumentForeignKey.OnDelete` is now typed as `Weasel.Core.CascadeAction`. The same four values (`NoAction`, `Cascade`, `SetNull`, `SetDefault`) remain available; Weasel.Core's version additionally exposes `Restrict`. Existing call sites only need a `using Weasel.Core;` addition:

```csharp
// before (Polecat 3.x)
mapping.ForeignKey<User>(x => x.AssigneeId, fk => fk.OnDelete = CascadeAction.Cascade);
//                                          ^ resolved Polecat.Storage.CascadeAction

// after (Polecat 4.0) — add `using Weasel.Core;` to the file
mapping.ForeignKey<User>(x => x.AssigneeId, fk => fk.OnDelete = CascadeAction.Cascade);
//                                          ^ resolves Weasel.Core.CascadeAction
```

#### `Polecat.Metadata.ITenanted` now extends `JasperFx.MultiTenancy.IHasTenantId`

Multi-tenancy dedup audit slice ([jasperfx#224](https://github.com/JasperFx/jasperfx/issues/224), row 1). `Polecat.Metadata.ITenanted` no longer declares its own `TenantId` property — it inherits it from `IHasTenantId`. Polecat-side framework code that accepts `IHasTenantId` now accepts any `ITenanted` document type.

Source-compatible for the typical case (your document class implementing `ITenanted` keeps a single `string TenantId { get; set; }` property and compiles unchanged). Only affects code that referenced the explicit `ITenanted.TenantId` property symbol via reflection or interface forwarding.

#### `Polecat.Exceptions.UnknownTenantException` → `JasperFx.MultiTenancy.UnknownTenantIdException`

Multi-tenancy dedup audit slice ([jasperfx#224](https://github.com/JasperFx/jasperfx/issues/224), row 2). The local `Polecat.Exceptions.UnknownTenantException` was removed; throw sites and consumers use the canonical `JasperFx.MultiTenancy.UnknownTenantIdException`. In Polecat 2.0.0-alpha.7 the JasperFx exception gained a `TenantId` property so consumers can `catch (UnknownTenantIdException ex) { ex.TenantId }` without parsing the message string.

```csharp
// before (Polecat 3.x)
catch (Polecat.Exceptions.UnknownTenantException ex)
{
    var id = ex.TenantId;
}

// after (Polecat 4.0)
catch (JasperFx.MultiTenancy.UnknownTenantIdException ex)
{
    var id = ex.TenantId;
}
```

### Event-sourcing API changes

#### `IInlineProjection.ApplyAsync` parameter widening

JasperFx.Events 2.0 widened `IInlineProjection<TOperations>.ApplyAsync(...)` so the `streams` parameter is `IEnumerable<StreamAction>` instead of `IReadOnlyList<StreamAction>` (jasperfx-events#4306). Polecat's built-in projection types (`NaturalKeyProjection`, `FlatTableProjection`) were updated to match; third-party `IInlineProjection<TOperations>` implementors must update their parameter type:

```csharp
// before
public Task ApplyAsync(TOperations operations,
                       IReadOnlyList<StreamAction> streams,
                       CancellationToken cancellation) { ... }

// after
public Task ApplyAsync(TOperations operations,
                       IEnumerable<StreamAction> streams,
                       CancellationToken cancellation) { ... }
```

If your implementation relied on `streams.Count` or indexed access, materialize the parameter once with `.ToList()` at the top of the method — the widening is intentional to give callers room to stream without forcing materialization.

#### `IJasperFxAggregateGrouper.Group` parameter tightening

JasperFx.Events 2.0 promoted the `events` parameter on `IJasperFxAggregateGrouper<TId, TQuerySession>.Group(...)` from `IEnumerable<IEvent>` to `IReadOnlyList<IEvent>` ([jasperfx#201](https://github.com/JasperFx/jasperfx/issues/201)). Polecat does not ship any `IAggregateGrouper` implementations of its own, so most Polecat users see no impact. If your application has custom groupers, update the parameter type — no logic change is required, and you can drop any defensive `events.ToList()` you'd been doing at the top of `Group`:

```csharp
// before
public Task Group(IQuerySession session,
                  IEnumerable<IEvent> events,
                  IEventGrouping<TId> grouping) { ... }

// after
public Task Group(IQuerySession session,
                  IReadOnlyList<IEvent> events,
                  IEventGrouping<TId> grouping) { ... }
```

### New behavior worth flagging

#### `BulkInsertWithVersionAsync` — optimistic concurrency on bulk inserts

[#48](https://github.com/JasperFx/polecat/issues/48) / [#62](https://github.com/JasperFx/polecat/pull/62). The new `BulkInsertMode.OverwriteIfVersionMatches` value (added by the Weasel.Core relocation) is exposed through a dedicated sibling method on `AdvancedOperations` rather than the existing `BulkInsertAsync` surface — the version-check needs per-row expected versions that the versionless overload has no way to thread:

```csharp
var batch = new[]
{
    (new User { Id = userId, FirstName = "Updated" }, expectedVersion: 1L),
    (new User { Id = newUserId, FirstName = "Fresh"  }, expectedVersion: 0L),
};

await store.Advanced.BulkInsertWithVersionAsync(batch);
```

Semantics:

- **Row exists + stored version == expected version** → UPDATE, version bumped to `target.version + 1`.
- **Row does not exist** → INSERT at version 1; the expected version is irrelevant on inserts.
- **Row exists + stored version != expected version** → MERGE is a no-op; after the batch's reader drains, `JasperFx.ConcurrencyException(typeof(T), id)` is thrown for the first mismatched id. Matches the per-row pattern used by `UpdateOperation` / `UpsertOperation`.

Calling the versionless `BulkInsertAsync(docs, BulkInsertMode.OverwriteIfVersionMatches)` overload now throws `InvalidOperationException` pointing callers at `BulkInsertWithVersionAsync` rather than the Polecat-3.x-era `NotSupportedException` stub.

::: warning
The concurrency throw is **best-effort, not transactional.** Matched-and-updated rows in the batch commit before the throw fires; the exception is the signal rather than a rollback. If you need transactional semantics, wrap the call in an outer transaction (`TransactionScope` or your own `BEGIN TRAN` / `ROLLBACK` flow).
:::

#### `FlatTableProjection` column casing on SQL Server

[#49](https://github.com/JasperFx/polecat/pull/49) (Weasel side: `99e40f0 fix(SqlServer): preserve user casing on TableColumn names`, shipped as Weasel 9.0.0-alpha.2). `TableColumn` names declared on a `FlatTableProjection` now preserve the casing you wrote in code; pre-fix the projection was silently lower-casing column names, breaking case-sensitive collations. No user action required — the projection schema is regenerated on next migration.

### AOT / codegen posture

Polecat 4 inherits the same AOT-friendly posture introduced in JasperFx 2.0 ([jasperfx#213](https://github.com/JasperFx/jasperfx/issues/213) AOT pillar, jasperfx#190 `ITypeLoader` abstraction). Polecat itself has been source-generator-first since 3.x — there is no Roslyn runtime-compile path to disable — so:

- `PublishAot=true` is the supported posture for Polecat 4 applications, modulo the usual System.Text.Json `JsonSerializerContext` setup for your document and event types.
- `IsAotCompatible=true` is now set on the Polecat assembly (PR [#67](https://github.com/JasperFx/polecat/pull/67)), and the reflective surfaces of Polecat have been progressively annotated for the trimmer / AOT analyzer — Serialization ([#74](https://github.com/JasperFx/polecat/pull/74)), ProjectionReplay ([#75](https://github.com/JasperFx/polecat/pull/75)), LINQ extension/provider surface ([#76](https://github.com/JasperFx/polecat/pull/76)), and Storage / Registry / EventStoreExplorer ([#77](https://github.com/JasperFx/polecat/pull/77)).

For the end-to-end "how do I publish AOT against the Critter Stack" walkthrough — recommended csproj flags, `WarningsAsErrors=IL*` setup, the source-generator-backed `ISerializer` swap, and the smoke-test pattern — see the **[Publishing AOT with JasperFx](https://jasperfx.github.io/codegen/aot)** guide on jasperfx.github.io. The guide is written for the whole Critter Stack; Polecat-specific call-outs are listed in the "Per-package status" table there.

### Out of scope (no Polecat 4 change)

- **Obsolete API removals.** A grep of the 3.x codebase for `[Obsolete]` declarations returned no matches; the 4.0 release does not remove any APIs that were obsoleted in 3.x.
- **Schema migrations.** No `pc_*` table shape changes between 3.x and 4.0. The same database that ran against 3.x continues to work against 4.0 without DDL changes.

### Dependency lockstep

Polecat 4 ships in lockstep with the rest of the Critter Stack 2026 wave. The supported pairings are:

| Polecat | Marten | JasperFx | JasperFx.Events | Weasel |
|---|---|---|---|---|
| 4.0 | 9.0 | 2.0 | 2.0 | 9.0 |

Mixing major versions across products is unsupported in this wave (the dedup work moves abstractions between assemblies and ABI-binds them to specific majors). If you upgrade Polecat to 4.0, plan to upgrade Marten and Weasel-side dependencies to their 9.0 lines at the same time.

## References

- [Polecat 4.0 master plan](https://github.com/JasperFx/polecat/issues/46)
- [Critter Stack 2026 umbrella](https://github.com/JasperFx/jasperfx/issues/217)
- [Marten 8 → 9 migration guide](https://martendb.io/migration-guide)
- [JasperFx 2.0 master plan](https://github.com/JasperFx/jasperfx/issues/215)
