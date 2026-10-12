# Native AOT

::: warning Native AOT works, with one documented exception and a contract you have to meet
A natively-published application can construct a store, migrate its schema, write and read
documents, use strong-typed document ids, append and read events, aggregate streams live, and run the
LINQ surface. **36 shapes are measured in CI.** 34 pass, and the two expected failures are a `GroupBy`
projected into an **anonymous type** ([#743](https://github.com/JasperFx/polecat/issues/743)), which
cannot work, and an **unregistered** strong-typed id, kept as the control that proves registering one
is required.

This is not a switch you turn on: you also have to meet [the consumer contract](#the-consumer-contract)
below — root your document types, supply a source-generated serializer, register each strong-typed
document id, and avoid `InvariantGlobalization`. Nothing here is estimated; `Polecat.AotRuntimeSmoke` publishes native and
runs every shape against a real SQL Server on each build.
:::

## What used to happen

Until 5.36.0, publishing with `PublishAot=true` succeeded and running the result did not:

```
System.NotSupportedException: 'Polecat.Storage.DocumentMapping.RawIdAccessors[
    JasperFx.Events.Daemon.DeadLetterEvent,System.Guid](System.Reflection.PropertyInfo)'
is missing native code. MethodInfo.MakeGenericMethod() is not compatible with AOT compilation.
```

**Every** operation failed the same way — document write, document read, `LoadAsync`, LINQ, event
append, event read, live aggregation. They all failed *before* doing anything, because
`DocumentStore`'s constructor builds a document mapping and `DocumentMapping` closed generics over the
document type at runtime. `DeadLetterEvent` was simply the first type to hit it, because the store
registers it eagerly; a consumer's own document types hit the same wall one step later.

That was [#733](https://github.com/JasperFx/polecat/issues/733), closed across #739, #740 and #742 by
peeling one layer at a time — publish native, run, see where it stops next — rather than by reasoning
about it. The history is worth keeping because it is what the measurements below are measured
*against*, and because its conclusion still governs the one shape left: an instantiation whose
arguments are all **reference** types shares one canonical body and can be closed at runtime, while a
**value**-type argument cannot, so the fix was to close the id type statically rather than to annotate
anything.

## What changed in this release

**The LINQ async wrappers now tell you at compile time.** `PolecatQueryableExtensions` —
`ToListAsync`, `FirstAsync`, `CountAsync`, `AnyAsync`, `SumAsync` and the rest — carry
`[RequiresUnreferencedCode]` and `[RequiresDynamicCode]`:

```
warning IL3050: Using member 'Polecat.Linq.PolecatQueryableExtensions.ToListAsync<T>(...)'
which has 'RequiresDynamicCodeAttribute' can break functionality when AOT compiling.
```

Before this, those methods carried a class-level `[UnconditionalSuppressMessage]` for IL2026 /
IL2060 / IL3050 whose justification read *"the trimmer preserves those intrinsics"* — while the same
class's own remarks said AOT publishers should avoid them. Both could not be true, and the
suppression silenced the one diagnostic that would have told you. The result was a store that
compiled clean under `PublishAot` and threw on first use.

⚠️ That reasoning is still sound, but the *conclusion it reached about LINQ* no longer holds: #742
fixed the throwing, and #743 measured the surface as working. The annotation is therefore now
knowingly broader than the failures — see
[Why the LINQ surface still carries an AOT warning](#why-the-linq-surface-still-carries-an-aot-warning).

::: tip This changes nothing for a normal application
IL2026 and IL3050 are only reported when you are trimming or AOT-publishing. If you are not, you see
no new warnings.
:::

The annotation propagates, so these now carry it too: `ToPagedListAsync`, `AggregateToAsync`,
`HybridSearchAsync` / `HybridSearchWithScoresAsync` (its WebStyle text leg executes a LINQ query).

## What is *not* yet annotated

⚠️ **Clean compilation is not a promise of AOT safety anywhere in Polecat, in either direction.**
The keyed load path (`LoadAsync`), event reads (`FetchStreamAsync`, `AggregateStreamAsync`) and
document writes carry no AOT annotation at all — and all of them are now **measured as working**
(#742), so here the absence happens to be right. The annotated LINQ surface is the inverse: it warns
on shapes that work. Neither the presence nor the absence of a warning is evidence; only
`Polecat.AotRuntimeSmoke` is.

## How far it gets today

Measured, not estimated — `Polecat.AotRuntimeSmoke` publishes native and runs against a real SQL
Server. **36 shapes, 34 of which pass.** The two that do not are a `GroupBy` projected into an anonymous
type (#743) and, deliberately, a strong-typed id nobody registered (#751); everything else below works.

| | native |
|---|---|
| `DocumentStore` construction, including the built-in `DeadLetterEvent` registration | ✅ |
| schema migration — the run creates its own tables | ✅ |
| identity accessors, strong-typed id wrap/unwrap, identity assignment | ✅ |
| document **writes** | ✅ |
| **`LoadAsync`** — keyed reads | ✅ |
| **event append and `FetchStreamAsync`** | ✅ |
| **live aggregation** (`AggregateStreamAsync`) | ✅ |
| `Query<T>()` — LINQ reads, enum comparisons, child-collection filters | ✅ |
| `Count` / `Any` / `Sum` / `Min` / `Average` — the scalar aggregates | ✅ |
| `GroupBy`, including a value-type key, projected into a **named** type | ✅ |
| `GroupJoin(...).SelectMany(...)` | ✅ |
| a `where` value from a **method call**, a **member chain** or an **array literal** | ✅ |
| `Select` of a member — a `string`, an `enum`, a `Guid` id; on documents and on `QueryAllRawEvents()` | ✅ (5.37: value types fixed) |
| `Select` into a named type, or into an **anonymous** type without a `GroupBy` | ✅ |
| `Distinct`, `OrderBy`/`ThenBy`/`Skip`/`Take`, `FirstOrDefault`, `SingleOrDefault`, string `StartsWith`/`Contains` | ✅ |
| `ToPagedListAsync`, `ToJsonArrayAsync` | ✅ |
| `ToJsonPageByCursorAsync` — keyset cursor paging | ✅ (5.37: fixed) |
| `ModifiedSince` / `CreatedSince` | ✅ |
| soft deletes — the default filter, `MaybeDeleted`, `IsDeleted`, `DeletedSince` | ✅ |
| a `GroupBy` projected into an **anonymous type** | ❌ [#743](https://github.com/JasperFx/polecat/issues/743) |
| a **strong-typed** document id (`readonly record struct FooId(Guid)`): load, LINQ filter, `Select` | ✅ **once registered** (5.39, [#751](https://github.com/JasperFx/polecat/issues/751)) |
| a strong-typed document id that was **not** registered | ❌ see [Register strong-typed ids](#register-strong-typed-document-ids) |

### What makes the supported shapes work

Everywhere `TDoc` is already a type parameter and only the *id* type was closed at runtime, the four
canonical id types are now closed **statically** — `BuildTypedProvider<TDoc, Guid>` and its three
siblings are ordinary generic calls ILC can see and compile. The id type is still decided at runtime;
what changed is that the generic is no longer *closed* at runtime.

`MakeGenericType` / `MakeGenericMethod` can close an instantiation whose arguments are all
**reference** types, because those share one canonical body — so reaching these methods from a
runtime `Type` is fine. It is a **value-type** argument that has no compiled code, and an id type is
routinely `Guid`, `int` or `long`.

⚠️ A **strong-typed id** wrapper is a runtime value nothing in Polecat can name, so Polecat cannot close
its storage statically on your behalf. **You** can, with one line per document type — see
[Register strong-typed document ids](#register-strong-typed-document-ids). Plain `Guid` / `string` /
`int` / `long` ids need nothing.

### LINQ works, and the one shape that does not is a `GroupBy` into an anonymous type

`Query<T>()` reads, enum comparisons, child-collection filters, the scalar aggregates (`CountAsync`,
`AnyAsync`, `SumAsync`, `MinAsync`, `AverageAsync`), member and object projections, ordering and
paging, metadata and soft-delete filters, `GroupBy` and `GroupJoin(...).SelectMany(...)` all run in a
native image, measured by `Polecat.AotRuntimeSmoke`. There are no includes to measure: Polecat does not
implement `Include<T>()` ([marten-gaps.md](https://github.com/JasperFx/polecat/blob/main/marten-gaps.md)).

::: tip Two LINQ defects fixed in 5.37, both found by extending the measurement
- **A value-type `Select`** — `Select(x => x.Id)` over a `Guid`, an `enum`, an `int` — threw
  `NotSupportedException` ("missing native code"). The providers closed `ScalarListHandler<T>` over the
  element type with `MakeGenericType`, which has no compiled body for a value type. `ToListAsync<T>` now
  roots the instantiation while `T` is still a compile-time fact.
- **Keyset cursor paging** threw on every page that had a successor. The cursor codec called the
  reflection-default `JsonSerializer` overloads, which take no options and so never see your
  `JsonSerializerContext` — nothing you could register would have helped. It now writes with
  `Utf8JsonWriter`, byte-for-byte what it wrote before, so cursors your clients already hold stay valid.
:::

::: danger A `GroupBy` projected into an anonymous type cannot work
```cs
// ❌ fails in a native image
.GroupBy(q => q.Difficulty).Select(g => new { g.Key, Count = g.Count() })

// ✅ identical query, named projection — works
.GroupBy(q => q.Difficulty).Select(g => new Tally { Key = g.Key, Count = g.Count() })
```

`GroupByListHandler` deserializes the projection through `System.Text.Json`, which under Native AOT
needs a `JsonTypeInfo` from your source-generated context. It is specific to `GroupBy`: a plain
`Select(x => new { x.Title })` is measured as working, because its selector does not go through the
serializer. **You cannot supply one for an anonymous
type**, because `[JsonSerializable]` needs a nameable type — so unlike "root your document types"
below, this is not a contract you can satisfy. Project into a named type and register it with your
`JsonSerializerContext`.

Tracked on [#743](https://github.com/JasperFx/polecat/issues/743). ⚠️ Marten has the same latent
limitation: its AOT runtime smoke covers neither group-by nor anonymous projections, and its AOT
guide does not mention them, while its LINQ suppressions are justified on consumers supplying a
source-generated serializer — which is the assumption that fails here.
:::

### Why the LINQ surface still carries an AOT warning

[#743](https://github.com/JasperFx/polecat/issues/743) asked for `PolecatQueryableExtensions`'
`[RequiresDynamicCode]` to be narrowed to "the shapes that genuinely need a JIT". The first round of
measurement found that set **empty** — the where-clause shapes, the scalar aggregates, `GroupBy` and
`GroupJoin` all ran — and concluded that `MakeGenericType` on these paths only closed over reference
types.

⚠️ **That conclusion was wrong, and the annotation it would have removed was covering two real
failures.** Extending the measurement to the rest of the surface — metadata, soft deletes, paging and
the selectors — found that a value-type `Select` and keyset cursor paging both threw in a native
image (both fixed in 5.37; see above). Neither was reachable from the shapes the first round measured.
A warning removed then would have been removed over code that did not work.

The annotation stays, and that history is the argument. Removing a warning asserts safety across the
**whole** surface — 17 files in `Polecat/Linq` carry it — and what is measured is still a sample of
it, however much larger. Removing it would also mean putting a
class-level `UnconditionalSuppressMessage` back on the providers to silence the ~21
`MakeGenericType` sites, which is the exact form [#738](https://github.com/JasperFx/polecat/pull/738)
removed as the original defect.

So: **#738** established that a suppression which understates is dishonest. **#743** answered that an
annotation which overstates is the same sin inverted. Both are right, and there is a third — a
removal justified by partial measurement is the understating suppression again with extra steps. The
annotation is deliberately broader than the measured failure set, and saying so here is the
alternative to pretending otherwise in either direction.

⚠️ Marten resolves this differently: `src/Marten/Linq` carries no `[RequiresDynamicCode]` at all, and
32 of its files carry class-level `UnconditionalSuppressMessage` instead. Polecat is the stricter of
the two today. That is a deliberate divergence from the usual "mirror Marten" principle, not an
oversight.

Marten also documents two shapes that genuinely need a JIT and are worth avoiding here: a compiled
query with an `enum`-typed parameter — moot in Polecat, which
[will not implement compiled queries](https://github.com/JasperFx/polecat/blob/main/marten-gaps.md)
because SQL Server caches plans natively — and a `where` clause whose value comes from a method call,
which Polecat **measures as working**.

## The consumer contract

⚠️ Several walls found on the way turned out **not** to be Polecat defects: they are things only
your code can do, because only your code can name your types. If you publish natively you have to do
all of them.

### Root your document types

Polecat finds a document's identity by reflecting over its public properties. Nothing in your code
statically *reads* that property — the store assigns it and the store reads it — so the trimmer
removes it and startup fails:

```
InvalidOperationException: Document type 'Quest' must have a public property named 'Id'
or a property marked with [Identity] of type Guid, string, int, or long.
```

Polecat cannot fix this for you: it cannot name types it has never seen. Keep them alive yourself:

```cs
internal static class AotRoots
{
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Quest))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(QuestStarted))]
    internal static void Keep() { }
}
```

…called once at startup. One entry per document and event type. (`DeadLetterEvent` is the single type
Polecat *does* name, and it carries its own `DynamicDependency` for exactly this reason.)

### Supply a source-generated serializer

Native AOT disables reflection-based `System.Text.Json`, so every write fails with:

```
InvalidOperationException: Reflection-based serialization has been disabled for this application.
Either use the source generator APIs or explicitly configure the
'JsonSerializerOptions.TypeInfoResolver' property.
```

Declare a context and hand it to Polecat:

```cs
[JsonSerializable(typeof(Quest))]
[JsonSerializable(typeof(QuestStarted))]
[JsonSerializable(typeof(JasperFx.Events.Daemon.DeadLetterEvent))]
internal sealed partial class MyJsonContext : JsonSerializerContext;

// ...
opts.ConfigureSerialization(new JsonSerializerOptions { TypeInfoResolver = MyJsonContext.Default });
```

⚠️ **`DeadLetterEvent` is on that list and you would never guess it.** Polecat registers it as a
document type in `DocumentStore`'s constructor, so it crosses the serializer even in an application
that never touches dead letters. Every document type, every event type, and that one.

### Register strong-typed document ids

If a document's id is a strong-typed wrapper (Vogen, StronglyTypedId, or a plain
`readonly record struct`), register it, one line per document type:

```cs
// public readonly record struct BadgeId(Guid Value);
opts.RegisterValueTypeId<Badge, BadgeId, Guid>();
```

Polecat closes its document storage over the wrapper type at runtime. A wrapper is a **value** type,
and a native image has no compiled code for an instantiation over a value type that nothing named at
compile time, so without the call the first session to touch the document fails:

```
NotSupportedException: 'Weasel.Core.Identity.ValueTypeIdentification`3[Badge,BadgeId,System.Guid]'
is missing native code or metadata.
```

Naming all three types in your code is what makes the compiler emit them. The **document** type is in
the signature because the storage is closed over the document as well as the id, so an id-only
registration could not root it. This is the same API Marten has (`RegisterValueTypeId`, marten#5589).

Under a JIT the call does nothing and costs nothing, and an unregistered id still works through
reflection, so it is safe to add unconditionally. Load by id, a LINQ filter on the id and `Select` of
the id are all measured natively with the call in place, and an unregistered id is measured too, as
the expected failure that proves the call is necessary.

⚠️ The two escapes you would try first instead are both dead ends, established on #733 by trying
them: **reflecting** over the closed type's members fails because the trimmer removed the metadata
— you cannot reflect your way out of generics under AOT — and a `[DynamicDependency]` on the wrapper
preserves its members without instantiating anything.

### Do not use `InvariantGlobalization`

```
NotSupportedException: Globalization Invariant Mode is not supported.
```

The event-store paths need real globalization data, so `<InvariantGlobalization>true</InvariantGlobalization>`
breaks event append and aggregation. It is a tempting switch in an AOT project because it shrinks the
image; it is not available here.

## If you need AOT today

Meet the contract items above and a natively-published app can construct a store, migrate its schema,
write documents, load them by id — strong-typed ids included, once registered — append and read
events, aggregate a stream live, and run `Query<T>()`, including the scalar aggregates, `GroupBy` and
`GroupJoin`. The one thing you cannot do is project a `GroupBy` into an **anonymous type**
([#743](https://github.com/JasperFx/polecat/issues/743)); project it into a named type registered with
your `JsonSerializerContext` instead.

Measured rather than asserted — `Polecat.AotRuntimeSmoke` publishes native and runs every one of
those shapes against SQL Server on each build.

## How this is tested

Two CI lanes, with deliberately different jobs:

| lane | what it does | what it proves |
|---|---|---|
| `aot-smoke` | **builds** a static-mode consumer with IL2026/IL3050 promoted to errors | the AOT-*clean* surface has not regressed its annotations |
| `aot-runtime-smoke` | **publishes native and runs** against SQL Server | what actually happens, shape by shape |

A build-only lane cannot catch a runtime AOT failure, which is why the second one exists — and
`PolecatQueryableExtensions` suppressing its own IL3050 is exactly why `aot-smoke` was silent on the
path that broke.

**`aot-runtime-smoke` blocks, and each shape declares its expected outcome.** It used to be
`continue-on-error` because #733 made it fail wholesale; that flag came off in #752. The two
expected failures — the anonymous `GroupBy` (#743) and the deliberately unregistered strong-typed id
(#751) — are what lets the lane go green over known behaviour — and the lane is red whenever reality disagrees in either direction:

| the lane goes red when | because |
|---|---|
| a working shape fails | an AOT regression, which is the point |
| an **expected failure passes** | the gap closed and this page, the issue and the harness are now stale |
| an expected failure's message stops containing its documented reason | a database that is down is not evidence about AOT |

⚠️ That last row is why an expectation pins a substring rather than a bool. Without it, an expected
failure turns green for any unrelated break — on exactly the shape nobody is watching, because
failing *is* its expected outcome.

⚠️ **The previous arrangement gated nothing**, and that is the argument for this one rather than for
a shorter fix. Returning non-zero on any failure, over two shapes documented as failing, meant
permanently red; permanently red plus `continue-on-error` meant a regression in the other sixteen
shapes changed nothing a reader or a merge gate could see. A gate that cannot go green does not gate.

⚠️ Read the lane's failures before believing them. Two of the shapes added in #743 failed on their
first run for reasons that were the **harness's** fault, not Polecat's: one query was simply
unsupported (`GroupJoin` without a following `SelectMany`, which Polecat already refuses clearly),
and one projection type was missing from the smoke's own `JsonSerializerContext`. Only after fixing
both did the real limitation — that an anonymous projection *cannot* be registered — separate out.
