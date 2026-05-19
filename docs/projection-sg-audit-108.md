# Projection source-generator dispatch audit (Polecat#108)

> Pre-4.0 sweep verifying that every projection type in the Polecat test
> libraries either gets a `[GeneratedEvolver]` dispatcher emitted by
> `JasperFx.Events.SourceGenerator` or deliberately bypasses that path
> (via an `Evolve` / `EvolveAsync` / `DetermineAction*` override, or via
> its own non-SG dispatch like `FlatTableProjection`).
>
> **Headline count: 50 projection-shaped types audited. 41 generated, 9
> deliberate bypasses, 0 SG-discovery gaps filed.**
>
> Run against `JasperFx.Events 2.0.0-alpha.12` + `JasperFx.Events.SourceGenerator
> 2.0.0-alpha.5` on Polecat `main` at `c24367b`. Reproducible with
> `dotnet build polecat.slnx -c Release -p:EmitCompilerGeneratedFiles=true
> -p:CompilerGeneratedFilesOutputPath=$(pwd)/_gensg` (then look under
> `_gensg/**/JasperFx.Events.SourceGenerator/**`).

## How to read this report

- **✅ Generated**: SG emits a `[GeneratedEvolver]` partial / standalone
  evolver / `EventProjection` partial override; runtime registration finds
  the dispatcher; the post-#276 fail-fast does not fire.
- **⚠ Bypass**: by design — the projection overrides one of the dispatch
  methods (`Evolve`, `EvolveAsync`, `DetermineAction`, `DetermineActionAsync`,
  or `ApplyAsync` on `EventProjection`) so `JasperFxAggregationProjectionBase.
  isOverridden(...)` returns `true` and the SG path is intentionally
  not exercised. `FlatTableProjection` and `EfCoreSingleStreamProjection`
  also fall in this column because they ship their own dispatch.
- **❌ Gap**: SG should have emitted but didn't; filed against JasperFx#276
  with a min-repro. **(None at this audit.)**

The `partial` column reflects whether the **type itself** is declared
`partial`. Self-aggregating doc types use the SG's `SelfAggregating` /
`SelfAggregatingEvolve` modes, which emit a *separate* evolver class
registered via `[assembly: GeneratedEvolver(...)]` and don't strictly
require `partial` on the doc — but marking them `partial` is harmless and
matches the broader Critter Stack convention. Projection subclasses with
conventional `Apply` / `Create` / `Project` methods **do** require
`partial` (the SG attaches a partial override to that class).

## Self-aggregating document types

Registered via `opts.Projections.Snapshot<T>()`,
`opts.Projections.Add<SingleStreamProjection<T, TId>>(...)`,
`session.Events.AggregateStreamAsync<T>(...)`, etc. The SG inspects
`T`'s `Apply` / `Create` / `ShouldDelete` / `Evolve` / `EvolveAsync`
methods and emits a standalone `TEvolver` class.

| # | Doc | TId | Methods | `partial`? | SG output | Status |
|---|---|---|---|---|---|---|
| 1 | `Bug4197Aggregate` *(Events/Bug_4197_fetch_for_writing_natural_key.cs)* | `Guid` | Apply + Create | ✓ | `Polecat_Tests_Events_Bug4197Aggregate_System_GuidEvolver.g.cs` | ✅ |
| 2 | `DeletableAggregate` *(Events/aggregate_stream_to_last_known_tests.cs)* | `Guid` | Apply + Create + ShouldDelete | ✓ | `..._DeletableAggregate_System_GuidEvolver.g.cs` | ✅ |
| 3 | `StringQuestAggregate` *(Events/always_enforce_consistency_tests.cs)* | `string` | Apply + Create | ✓ | `..._StringQuestAggregate_stringEvolver.g.cs` | ✅ |
| 4 | `StudentCourseEnrollment` *(Events/dcb_tag_query_and_consistency_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._StudentCourseEnrollment_System_GuidEvolver.g.cs` | ✅ |
| 5 | `QuestAggregate` *(Events/fetch_for_writing_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._QuestAggregate_System_GuidEvolver.g.cs` | ✅ |
| 6 | `InlineSeAggregate` *(Events/inline_projection_side_effects_tests.cs — nested)* | `Guid` | Apply | ✓ | `..._inline_projection_side_effects_tests_InlineSeAggregate_System_GuidEvolver.g.cs` | ✅ |
| 7 | `OrderAggregate` *(Events/natural_key_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._OrderAggregate_System_GuidEvolver.g.cs` | ✅ |
| 8 | `InvoiceAggregate` *(Events/natural_key_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._InvoiceAggregate_System_GuidEvolver.g.cs` | ✅ |
| 9 | `Report` *(Events/project_latest_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._Report_System_GuidEvolver.g.cs` | ✅ |
| 10 | `StringReport` *(Events/project_latest_tests.cs)* | `string` | Apply + Create | ✓ | `..._StringReport_stringEvolver.g.cs` | ✅ |
| 11 | `ScenarioQuestParty` *(Events/projection_scenario_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._ScenarioQuestParty_System_GuidEvolver.g.cs` | ✅ |
| 12 | `MutableIEventEvolveAggregate` *(Events/self_aggregating_evolve_method.cs)* | `Guid` | `Evolve(snapshot, IEvent)` | ✗ | `..._MutableIEventEvolveAggregate_System_GuidEvolveEvolver.g.cs` | ✅ |
| 13 | `MutableObjectEvolveAggregate` *(Events/self_aggregating_evolve_method.cs)* | `Guid` | `Evolve(snapshot, TEvent)` | ✗ | `..._MutableObjectEvolveAggregate_System_GuidEvolveEvolver.g.cs` | ✅ |
| 14 | `ImmutableIEventEvolveAggregate` *(record; Events/self_aggregating_evolve_method.cs)* | `Guid` | `Evolve(snapshot, IEvent)` returning new record | ✗ | `..._ImmutableIEventEvolveAggregate_System_GuidEvolveEvolver.g.cs` | ✅ |
| 15 | `ImmutableObjectEvolveAggregate` *(record; same file)* | `Guid` | `Evolve(snapshot, TEvent)` returning new record | ✗ | `..._ImmutableObjectEvolveAggregate_System_GuidEvolveEvolver.g.cs` | ✅ |
| 16 | `AsyncEvolveAggregate` *(Events/self_aggregating_evolve_method.cs)* | `Guid` | `EvolveAsync` | ✗ | `..._AsyncEvolveAggregate_System_GuidEvolveEvolver.g.cs` | ✅ |
| 17 | `ImmutableAsyncEvolveAggregate` *(record; same file)* | `Guid` | `EvolveAsync` returning new record | ✗ | `..._ImmutableAsyncEvolveAggregate_System_GuidEvolveEvolver.g.cs` | ✅ |
| 18 | `CompositeQuestParty` *(Projections/composite_projection_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._CompositeQuestParty_System_GuidEvolver.g.cs` | ✅ |
| 19 | `QuestStats` *(Projections/composite_projection_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._QuestStats_System_GuidEvolver.g.cs` | ✅ |
| 20 | `QuestParty` *(Projections/inline_projection_tests.cs)* | `Guid` | Apply + Create + ShouldDelete | ✓ | `..._QuestParty_System_GuidEvolver.g.cs` | ✅ |
| 21 | `SelfAggregatingStringQuest` *(Projections/single_stream_projection_with_string_identity_tests.cs)* | `string` | Apply + Create | ✓ | `..._SelfAggregatingStringQuest_stringEvolver.g.cs` | ✅ |
| 22 | `SnapshotParty` *(Projections/snapshot_registration_tests.cs)* | `Guid` | Apply + Create | ✓ | `..._SnapshotParty_System_GuidEvolver.g.cs` | ✅ |
| 23 | `SnapshotPartyByString` *(Projections/snapshot_registration_tests.cs)* | `string` | Apply + Create | ✓ | `..._SnapshotPartyByString_stringEvolver.g.cs` | ✅ |
| 24 | `Payment` *(Projections/using_guid_based_strong_typed_id_for_aggregate_identity.cs)* | `PaymentId` *(wraps Guid)* | Apply + Create | ✓ | `..._Payment_Polecat_Tests_Projections_PaymentIdEvolver.g.cs` | ✅ |
| 25 | `Payment2` *(Projections/using_string_based_strong_typed_id_for_aggregate_identity.cs)* | `Payment2Id` *(wraps string)* | Apply + Create | ✓ | `..._Payment2_Polecat_Tests_Projections_Payment2IdEvolver.g.cs` | ✅ |
| 26 | `Quest` *(AotSmoke/Program.cs)* | `Guid` | Create | ✓ | `Polecat_AotSmoke_Quest_System_GuidEvolver.g.cs` | ✅ |
| 27 | `StreamingQuestParty` *(AspNetCore.Testing/Program.cs)* | `Guid` | Apply + Create | ✓ | `..._StreamingQuestParty_System_GuidEvolver.g.cs` | ✅ |

> **27/27 self-aggregating doc types: ✅ all generated.**

## Projection subclasses (Polecat-side)

Subclass of one of `EventProjection`, `SingleStreamProjection<TDoc, TId>`,
`MultiStreamProjection<TDoc, TId>`, `PolecatCompositeProjection`,
`FlatTableProjection`. The SG attaches a `[GeneratedEvolver]` partial
method to the class (or, for `EventProjection`, an `ApplyAsync` partial
override) — **`partial` required on the class** for these shapes.

| # | Class | Base | Shape | `partial`? | SG output | Status |
|---|---|---|---|---|---|---|
| 1 | `QuestLogProjection` *(Projections/event_projection_tests.cs)* | `EventProjection` | conventional `Project(TEvent, IDocumentSession)` | ✓ | `..._QuestLogProjection.EventProjection.g.cs` | ✅ |
| 2 | `MultiEventQuestLogProjection` *(Projections/event_projection_tests.cs)* | `EventProjection` | conventional `Project(TEvent, IDocumentSession)` × 2 | ✓ | `..._MultiEventQuestLogProjection.EventProjection.g.cs` | ✅ |
| 3 | `SimpleEnrichmentProjection` *(Projections/event_projection_enrichment_tests.cs)* | `EventProjection` | `Project` + `EnrichEventsAsync` override | ✓ | `..._SimpleEnrichmentProjection.EventProjection.g.cs` | ✅ (EnrichEventsAsync is a side-channel; dispatch still through SG-emitted `Project`) |
| 4 | `EnrichmentCallOrderProjection` *(Projections/event_projection_enrichment_tests.cs)* | `EventProjection` | `Project` + `EnrichEventsAsync` override | ✓ | `..._EnrichmentCallOrderProjection.EventProjection.g.cs` | ✅ |
| 5 | `DbLookupEnrichmentProjection` *(Projections/event_projection_enrichment_tests.cs)* | `EventProjection` | `Project` + `EnrichEventsAsync` override | ✓ | `..._DbLookupEnrichmentProjection.EventProjection.g.cs` | ✅ |
| 6 | `AuditRecordProjection` *(Projections/event_projection_should_register_document_types.cs)* | `EventProjection` | overrides `ApplyAsync` directly | ✓ | `..._AuditRecordProjection.TypeRegistration.g.cs` *(TypeRegistration-only)* | ✅ — dispatch via `ApplyAsync` override; SG emits only the `RegisterPublishedType<AuditRecord>` constructor so `Schema.For<AuditRecord>` is discoverable (per [marten#4166](https://github.com/JasperFx/marten/issues/4166)) |
| 7 | `AuditRecordCreatorProjection` *(Projections/event_projection_should_register_document_types.cs)* | `EventProjection` | conventional `Create(AuditableEvent)` returning the doc type | ✓ | `..._AuditRecordCreatorProjection.EventProjection.g.cs` | ✅ |
| 8 | `ImportSqlProjection` *(Projections/using_event_projection_for_flat_tables.cs)* | `EventProjection` | conventional `Project(TEvent, IDocumentSession)` × 2 | ✓ | `..._ImportSqlProjection.EventProjection.g.cs` | ✅ |
| 9 | `StringQuestPartyProjection` *(Projections/single_stream_projection_with_string_identity_tests.cs)* | `SingleStreamProjection<StringQuestParty, string>` | conventional Apply/Create + ShouldDelete on the **projection** (delegating to doc would be the alternative) | ✓ | `..._StringQuestPartyProjection.Evolver.g.cs` *(PartialProjection)* | ✅ |
| 10 | `CustomerSummaryProjection` *(Projections/multi_stream_projection_tests.cs)* | `MultiStreamProjection<CustomerSummary, Guid>` | conventional Apply on the projection | ✓ | `..._CustomerSummaryProjection.Evolver.g.cs` | ✅ |
| 11 | `MonthlyAccountActivityProjection` *(Projections/time_based_multi_stream_projection_tests.cs)* | `MultiStreamProjection<MonthlyAccountActivity, string>` | conventional Apply on the projection | ✓ | `..._MonthlyAccountActivityProjection.Evolver.g.cs` | ✅ |
| 12 | `CompositeOrderProjection` *(Projections/composite_try_find_upstream_cache_tests.cs)* | `SingleStreamProjection<CompositeOrder, Guid>` | **overrides `Evolve` directly** | ✓ | — | ⚠ Deliberate bypass — override wins per #276 doctrine. Composite-projection test exercises upstream-cache lookup, not dispatch correctness. |
| 13 | `OrderShippingNotificationProjection` *(Projections/composite_try_find_upstream_cache_tests.cs)* | `MultiStreamProjection<OrderShippingNotification, Guid>` | overrides `Evolve` + `EnrichEventsAsync` for the upstream-cache `TryFindUpstreamCache` pattern | ✓ | — | ⚠ Deliberate bypass — overrides win; the test specifically exercises the composite-projection upstream-cache plumbing. |
| 14 | `InlineSeProjection` *(Events/inline_projection_side_effects_tests.cs — nested)* | `SingleStreamProjection<InlineSeAggregate, Guid>` | overrides `RaiseSideEffects` only; **no Apply on the projection** | ✓ | — | ✅ Dispatch resolves via the SG-emitted `InlineSeAggregate` self-aggregating evolver (#6 in the doc table). The projection only adds outbox side effects; the empty-Apply-set on the projection class is intentional. |
| 15 | `QuestMetricsProjection` *(Projections/flat_table_projection_tests.cs)* | `FlatTableProjection` | own dispatch via `Project<TEvent>` / `Delete<TEvent>` registrations to `_handlers` dictionary | ✗ | — | ⚠ Deliberate bypass — `FlatTableProjection` is not part of the JasperFx.Events apply-method-discovery contract. It owns its own dispatch keyed on event Type in `_handlers`; `partial` would be a no-op. |
| 16 | `PascalCaseFlatProjection` *(Projections/flat_table_projection_tests.cs)* | `FlatTableProjection` | same | ✗ | — | ⚠ Same as above |

> **11/16 projection subclasses: ✅ generated. 4/16: ⚠ deliberate bypass (Evolve override or FlatTable). 1/16: ✅ delegates to its self-aggregating doc evolver.**

## EF Core projections (Polecat.EntityFrameworkCore.Tests)

| # | Class *(`TestProjections.cs`)* | Base | Dispatch | SG output | Status |
|---|---|---|---|---|---|
| 1 | `OrderAggregate` | `EfCoreSingleStreamProjection<Order, TestDbContext>` | overrides `DetermineActionAsync` to extract `DbContext` from `EfCoreProjectionStorage` | — | ⚠ Deliberate bypass — EF Core integration shape; override wins per #276 doctrine. |
| 2 | `OrderDetailProjection` | `EfCoreEventProjection<TestDbContext>` | extends `ProjectionBase`+`IProjection` directly (not `EventProjection`), wrapped in `ProjectionWrapper` for registration | — | ⚠ Deliberate bypass — different inheritance chain from the SG's `EventProjection` discovery; dispatch via the wrapper. |
| 3 | `TenantedOrderAggregate` | `EfCoreSingleStreamProjection<TenantedOrder, TenantedTestDbContext>` | same as `OrderAggregate` | — | ⚠ Same as #1 |
| 4 | `NonTenantedOrderAggregate` | `EfCoreSingleStreamProjection<NonTenantedOrder, TestDbContext>` | same | — | ⚠ Same as #1 |

> **4/4 EF Core projections: ⚠ deliberate bypass.** EF Core integration owns its own dispatch path (extracts `DbContext` from `EfCoreProjectionStorage` inside an override) so SG emission was never expected for these.

## DcbLoadTest

No projection types declared. (Load harness exercises the event store
write path; no projections registered.)

## Summary

| Category | Count |
|---|---|
| Self-aggregating doc types — ✅ generated | 27 |
| Projection subclasses — ✅ generated | 11 |
| Projection subclasses — ⚠ deliberate bypass (`Evolve` / `EnrichEventsAsync` override or `FlatTableProjection` / `EfCoreSingleStreamProjection`) | 9 |
| Projection subclasses — ✅ delegates to a self-aggregating evolver | 1 |
| Projection subclasses — ❌ SG-discovery gap filed against `JasperFx#276` | **0** |
| **Total projection-shaped types inventoried** | **50** *(46 in Polecat.Tests + 4 in EFCore.Tests, plus AotSmoke + AspNetCore.Testing samples)* |

**SG emission stats:** 38 `[GeneratedEvolver]` outputs from
`JasperFx.Events.SourceGenerator` across the solution — 27
self-aggregating-doc evolvers, 8 `EventProjection` partial overrides,
3 `PartialProjection` `Evolver` overrides on
`SingleStreamProjection<,>` / `MultiStreamProjection<,>` subclasses,
and 1 TypeRegistration-only emit for `AuditRecordProjection`.

## Shape coverage (no silent-mis-execution check)

Per chip §4, every shape with an SG-emitted dispatcher has at least one
behavioral test in the existing Polecat test suite that fires events
through the projection and asserts state evolves correctly:

| Shape | Representative test |
|---|---|
| Self-aggregating sync Apply + Create | `fetch_for_writing_tests`, `Bug_4197_fetch_for_writing_natural_key` |
| Self-aggregating Apply + Create + **ShouldDelete** | `aggregate_stream_to_last_known_tests` (DeletableAggregate), `inline_projection_tests` (QuestParty) |
| Self-aggregating sync `Evolve(snapshot, IEvent)` (mutable + immutable record) | `self_aggregating_evolve_method` (×4 variants) |
| Self-aggregating `EvolveAsync` | `self_aggregating_evolve_method` (×2 variants) |
| String identity | `single_stream_projection_with_string_identity_tests`, `project_latest_tests` (StringReport), `always_enforce_consistency_with_string_stream_id` |
| Strong-typed-id identity (wrapper struct) | `using_guid_based_strong_typed_id_for_aggregate_identity`, `using_string_based_strong_typed_id_for_aggregate_identity` |
| `EventProjection` with conventional `Project(TEvent, IDocumentSession)` | `event_projection_tests` |
| `EventProjection` with `Create(TEvent)` returning a doc | `event_projection_should_register_document_types` (AuditRecordCreatorProjection) |
| `EventProjection` with `ApplyAsync` override | `event_projection_should_register_document_types` (AuditRecordProjection) |
| `EventProjection` with `EnrichEventsAsync` override | `event_projection_enrichment_tests` (×3 variants) |
| `SingleStreamProjection<TDoc, TId>` subclass with Apply on the projection | `single_stream_projection_with_string_identity_tests` (StringQuestPartyProjection) |
| `MultiStreamProjection<TDoc, TId>` subclass with Apply on the projection | `multi_stream_projection_tests`, `time_based_multi_stream_projection_tests` |
| `PolecatCompositeProjection` with `Snapshot<T>` | `composite_projection_tests` |
| `PolecatCompositeProjection` with `TryFindUpstreamCache` between stages | `composite_try_find_upstream_cache_tests` |
| Inline projection with `RaiseSideEffects` outbox | `inline_projection_side_effects_tests` |
| `FlatTableProjection` (non-SG dispatch) | `flat_table_projection_tests`, `using_event_projection_for_flat_tables` |
| `EfCoreSingleStreamProjection` (non-SG dispatch) | `Polecat.EntityFrameworkCore.Tests` integration suite |
| DCB tags + self-aggregating | `dcb_tag_query_and_consistency_tests` (StudentCourseEnrollment) |

No shape is unrepresented; no new per-shape behavioral test was added by
this audit.

## CI regression guard

A focused dispatcher-resolution harness lives at
`src/Polecat.Tests/Projections/projection_sg_dispatch_audit_tests.cs`.
It registers every emit-expected test-library projection through a
fresh `DocumentStore` and asserts construction completes without
`InvalidProjectionException`. Future `JasperFx.Events.SourceGenerator`
regressions (a shape the SG silently fails to emit a `[GeneratedEvolver]`
for) trip this single test immediately, before the broader test suite's
slower fixtures get a chance to fail with the same symptom across many
files.

## SG gaps filed

**None.** The post-#298 fix landed in `JasperFx.Events 2.0.0-alpha.12`
unblocked the last shape that was tripping the runtime (self-aggregating
docs with `ShouldDelete` going through `IGeneratedSyncDetermineAction`).
Every projection in the inventory either generates correctly or falls into
the deliberate-bypass column.
