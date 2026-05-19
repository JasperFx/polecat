# Migration Guide

## Key Changes in 4.0.0

Polecat 4.0 ships in lockstep with [Marten 9.0](https://martendb.io) and [JasperFx 2.0](https://github.com/JasperFx/jasperfx/issues/215) as part of the [Critter Stack 2026](https://github.com/JasperFx/jasperfx/issues/217) release wave. The 4.0 line builds on the same JasperFx 2.0 / JasperFx.Events 2.0 / Weasel 9.0 alphas that Marten 9 consumes — this is the consolidation cycle, not a rewrite. Most upgrades are pin bumps; the breaking surface is small and localized.

### Foundation pin bumps

Polecat 4 consumes the 2026-wave alpha line for the shared substrate. Update your `Directory.Packages.props` (or the equivalent in your csproj):

| Package | 3.x | 4.0 (current alpha) |
|---|---|---|
| `JasperFx` | `1.31.0` | `2.0.0-alpha.16` |
| `JasperFx.Events` | `1.36.0` | `2.0.0-alpha.15` |
| `JasperFx.Events.SourceGenerator` | `1.4.0` | `2.0.0-alpha.8` |
| `JasperFx.RuntimeCompiler` *(transitive, but pin centrally for matrix coherence)* | *(no equivalent)* | `5.0.0-alpha.4` |
| `JasperFx.SourceGeneration` *(transitive, but pin centrally)* | *(no equivalent)* | `2.0.0-alpha.5` |
| `Weasel.SqlServer` | `8.15.2` | `9.0.0-alpha.5` |
| `Weasel.EntityFrameworkCore` | `8.15.2` | `9.0.0-alpha.5` |

The alpha line is still rolling forward as the 2026 wave converges; the table above reflects the current Polecat 4 pins (matched against the same set Marten 9.0.0-alpha.\* consumes). Expect another tick or two before 4.0 GA — keep one set of bumps in your renovate/dependabot config and pin all seven packages together.

::: warning
Pin the **`5.x` line** of `JasperFx.RuntimeCompiler` — that's the active continuation of the 4.x lineage. A parallel `2.0.x` series exists on NuGet but is **stale**; don't pin against it.
:::

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

#### Projections and self-aggregating documents must be `partial`

[JasperFx/jasperfx#276](https://github.com/JasperFx/jasperfx/issues/276) (FEC elimination, Phase 1) removed the FastExpressionCompiler fallback in `JasperFx.Events`'s projection apply-method dispatch. Source-generated dispatchers emitted by [`JasperFx.Events.SourceGenerator`](https://www.nuget.org/packages/JasperFx.Events.SourceGenerator) are now the only path — runtime registration **fails fast** when no `[GeneratedEvolver]` is found for a projection that uses conventional `Apply` / `Create` / `ShouldDelete` methods:

```
JasperFx.Events.Projections.InvalidProjectionException:
  No source-generated dispatcher found for MyApp.MyProjection.
  When using conventional Apply/Create/ShouldDelete methods, the projection class must be
  declared `partial` in an assembly that references the JasperFx.Events.SourceGenerator analyzer,
  or alternatively override Evolve / EvolveAsync / DetermineAction / DetermineActionAsync directly.
```

To clear this exception, **every projection class that uses conventional apply-method discovery must be `partial`**, including:

- Subclasses of `SingleStreamProjection<TDoc, TId>`, `MultiStreamProjection<TDoc, TId>`, `EventProjection`, and `PolecatCompositeProjection`.
- **Self-aggregating document types** registered via `opts.Projections.Snapshot<T>(...)`, queried via `session.Events.AggregateStreamAsync<T>(...)`, or live-projected via `session.Events.FetchLatest<T>(...)` / `FetchForWriting<T>(...)`. The source generator emits a standalone `TEvolver` class for the closed `SingleStreamProjection<T, T.IdType>` runtime instance, and it can only do that when `T` is `partial`.

```csharp
// before (Polecat 3.x — works because of FEC fallback)
public class QuestParty
{
    public Guid Id { get; set; }
    public List<string> Members { get; } = [];

    public static QuestParty Create(QuestStarted e) => new();
    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
}

// after (Polecat 4.0 — `partial` required)
public partial class QuestParty
{
    public Guid Id { get; set; }
    public List<string> Members { get; } = [];

    public static QuestParty Create(QuestStarted e) => new();
    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
}
```

Every assembly that defines such a projection or aggregate also needs to reference the `JasperFx.Events.SourceGenerator` NuGet as an **analyzer-only** package — Polecat itself already pulls it in through `JasperFx.Events` for projection types declared in your store-host project, but any sibling assembly that declares its own projection / aggregate types needs its own reference:

```xml
<ItemGroup>
  <PackageReference Include="JasperFx.Events.SourceGenerator"
                    OutputItemType="Analyzer"
                    ReferenceOutputAssembly="false" />
</ItemGroup>
```

If you have a projection that **cannot** be made `partial` (e.g. a sealed class from a third-party library, or one you want to keep as a non-source-generator path), the escape hatch is to override the dispatch methods directly:

```csharp
public class MyProjection : SingleStreamProjection<MyAggregate, Guid>
{
    public override ValueTask<MyAggregate?> EvolveAsync(
        MyAggregate? snapshot, Guid id, IEvent e, CancellationToken ct)
    {
        // hand-written dispatch — no SG, no FEC
    }
}
```

This matches Marten 9's adoption story for the same JasperFx.Events Phase-1 change. The shared consumer-side contract is documented at greater length in [Marten's migration guide — "Projection apply dispatch is now source-generator-only"](https://martendb.io/migration-guide#projection-apply-dispatch); the Polecat-side adoption tracking lives in [Polecat#46](https://github.com/JasperFx/polecat/issues/46), with the audit harness from [#110](https://github.com/JasperFx/polecat/pull/110) standing as the pre-release gate against future SG regressions.

##### Convention-method visibility, identity, and required members

The source generator inspects only **public** instance / static methods. `Apply` / `Create` / `ShouldDelete` methods that were `private` / `internal` / `protected` under Polecat 3.x (the reflective FEC path picked those up) are silently skipped by the SG and trip the fail-fast above. Make them `public`:

```csharp
// before (Polecat 3.x — FEC reflected over non-public members too)
public partial class Quest
{
    public Guid Id { get; set; }
    private void Apply(MembersJoined e) { /* ... */ }   // <-- skipped by SG
}

// after (Polecat 4.0)
public partial class Quest
{
    public Guid Id { get; set; }
    public void Apply(MembersJoined e) { /* ... */ }    // <-- discovered
}
```

Aggregate identity discovery follows the same convention rule as Marten — by default, the SG looks for a property literally named `Id` (or `<TypeName>Id`). If your aggregate's identity uses a different member name, annotate it with `[Identity]` from `JasperFx.Events`:

```csharp
using JasperFx.Events;

public partial class Quest
{
    [Identity]
    public Guid QuestKey { get; set; }   // <-- explicit identity slot

    public static Quest Create(QuestStarted e) => new() { QuestKey = e.QuestId };
    public void Apply(MembersJoined e) { /* ... */ }
}
```

For aggregates with `required` members, the SG's null-snapshot "create-from-default + apply" branch can't `new T()` directly — `required` members would leave the compiler complaining. Either provide a static `Create(TEvent e)` factory (preferred — gives full control) or use the `default!` init-pattern Marten's guide documents:

```csharp
public partial class Account
{
    public Guid Id { get; set; }
    public required string Owner { get; init; }

    // Preferred: explicit factory — SG calls this, no default! required.
    public static Account Create(AccountOpened e) => new()
    {
        Id = e.AccountId,
        Owner = e.Owner,
    };

    public void Apply(Deposited e) { /* ... */ }
}
```

If no `Create` factory is supplied and the SG falls back to the create-from-default path, the emitted code is `var s = new Account { Owner = default! }; Apply(e, s);` — semantically valid, but the `default!` hands the aggregate over to your `Apply` methods in a partially-initialized state. The factory route is cleaner.

##### Surfaces unaffected by the partial requirement

- **`FlatTableProjection`** — has its own dictionary-keyed dispatch via `Project<TEvent>(...)` / `Delete<TEvent>(...)`, doesn't go through JasperFx.Events apply-method discovery. No `partial` required.
- **`EfCoreSingleStreamProjection<TDoc, TDbContext>` / `EfCoreEventProjection<TDbContext>`** (from `Polecat.EntityFrameworkCore`) — override `DetermineActionAsync` directly, also bypass the SG-required path.

#### Inline-lambda projection registration APIs removed

[JasperFx/jasperfx#276](https://github.com/JasperFx/jasperfx/issues/276) / [#286](https://github.com/JasperFx/jasperfx/issues/286) removed the inline-lambda registration overloads on `EventProjection`, `IAggregationSteps<T, TQuerySession>`, and `JasperFxAggregationProjectionBase` — the source generator cannot dispatch a runtime closure, so these were the last thing keeping FEC reachable. Migrate to conventional method declarations on the projection class:

```csharp
// before (Polecat 3.x)
public class MyEventProjection : EventProjection
{
    public MyEventProjection()
    {
        Project<OrderPlaced>((e, ops) =>
        {
            ops.Store(new OrderSummary { Id = e.OrderId, ... });
        });
    }
}

// after (Polecat 4.0)
public partial class MyEventProjection : EventProjection
{
    public void Project(OrderPlaced e, IDocumentSession ops)
    {
        ops.Store(new OrderSummary { Id = e.OrderId, ... });
    }
}
```

The same shape — `public void Project(TEvent, IDocumentSession)` for inline mutation, plus the conventional `Create` / `Apply` / `ShouldDelete` methods for aggregations — covers everything the lambda APIs used to do. `DeleteEvent<TEvent>()` (no-args, populates the internal delete-types list) and `TransformsEvent<TEvent>()` remain available; only the lambda-taking overloads were dropped.

The migration recipe is identical between Polecat and Marten — see [Marten's migration guide — "Inline-lambda projection registration APIs removed"](https://martendb.io/migration-guide#inline-lambda-removed) for the same content + a longer worked example.

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
- `IsAotCompatible=true` is now set on the Polecat assembly (PR [#67](https://github.com/JasperFx/polecat/pull/67)), and the reflective surfaces of Polecat have been progressively annotated for the trimmer / AOT analyzer — Serialization ([#74](https://github.com/JasperFx/polecat/pull/74)), ProjectionReplay ([#75](https://github.com/JasperFx/polecat/pull/75)), LINQ extension/provider surface ([#76](https://github.com/JasperFx/polecat/pull/76)), Storage / Registry / EventStoreExplorer ([#77](https://github.com/JasperFx/polecat/pull/77)), and the class-level → call-site refactor of the remaining reflective surface ([#107](https://github.com/JasperFx/polecat/pull/107)).
- **FastExpressionCompiler is no longer pulled in transitively** through `JasperFx.Events` ([jasperfx#276](https://github.com/JasperFx/jasperfx/issues/276)). Projection apply-method dispatch routes exclusively through `[GeneratedEvolver]` outputs from `JasperFx.Events.SourceGenerator` — see the **"Projections and self-aggregating documents must be `partial`"** section above for the consumer-side migration.
- An AOT smoke-test consumer ([Polecat.AotSmoke](https://github.com/JasperFx/polecat/tree/main/src/Polecat.AotSmoke), PR [#106](https://github.com/JasperFx/polecat/pull/106)) ships with the repo and runs in CI with `WarningsAsErrors` covering `IL2026 / IL2046 / IL2055 / IL2065 / IL2067 / IL2070 / IL2072 / IL2075 / IL2090 / IL2091 / IL2111 / IL3050 / IL3051`. Regressions in Polecat's AOT-clean surface fail the build.

### Publishing AOT

Polecat 4 applications target the same Critter Stack 2026 AOT pre-gen flow Marten 9 uses — Polecat is a thin consumer of `JasperFx.Events`, so the consumer-side recipe (csproj flags, `WarningsAsErrors=IL*` set, source-generator-backed `ISerializer` swap, projection-class partial requirement, smoke-test pattern) lives in JasperFx's docs rather than being forked here:

- **[Publishing AOT with JasperFx](https://jasperfx.github.io/codegen/aot)** — the end-to-end Critter Stack guide. Read this first.
- **[Marten's AOT publishing walkthrough](https://martendb.io/configuration/aot-publishing)** — longer worked example with full csproj + program snippets. Polecat-applicable apart from the Marten-specific service registrations.

Polecat-specific call-outs:

- **`Polecat.AotSmoke`** ([source](https://github.com/JasperFx/polecat/tree/main/src/Polecat.AotSmoke), originally [PR #106](https://github.com/JasperFx/polecat/pull/106)) ships in-tree as the canonical AOT consumer example. The csproj sets `IsAotCompatible=true` + `TrimMode=full` and promotes IL2026 / IL2046 / IL2055 / IL2065 / IL2067 / IL2070 / IL2072 / IL2075 / IL2090 / IL2091 / IL2111 / IL3050 / IL3051 to errors — any regression in Polecat's AOT-clean surface fails the build.
- **`IsAotCompatible=true`** is set on `Polecat.csproj` itself.
- The **post-#276 fail-fast** semantics apply identically to AOT consumers — a projection registered without a `[GeneratedEvolver]` dispatcher throws `InvalidProjectionException` at host build (not at runtime via FEC fallback like Polecat 3.x). The remediation is the same as the non-AOT story — see the [SG-only projection apply dispatch](#projections-and-self-aggregating-documents-must-be-partial) section above.

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
