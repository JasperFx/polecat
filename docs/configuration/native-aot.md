# Native AOT

::: danger Polecat does not currently work under Native AOT
A natively-published application throws on its first database operation, from `DocumentStore`'s
constructor. This is [polecat#733](https://github.com/JasperFx/polecat/issues/733), and this page
documents the current state rather than a configuration you can turn on.
:::

## What happens

Publishing with `PublishAot=true` succeeds. Running the result does not:

```
System.NotSupportedException: 'Polecat.Storage.DocumentMapping.RawIdAccessors[
    JasperFx.Events.Daemon.DeadLetterEvent,System.Guid](System.Reflection.PropertyInfo)'
is missing native code. MethodInfo.MakeGenericMethod() is not compatible with AOT compilation.
```

Every operation fails the same way — document write, document read, `LoadAsync`, LINQ, event append,
event read, live aggregation. They all fail *before* doing anything, because `DocumentStore`'s
constructor builds a document mapping, and `DocumentMapping` closes generics over the document type
at runtime. `src/Polecat` has **108** `MakeGenericType` / `MakeGenericMethod` sites, 21 of them in
the LINQ provider alone.

`DeadLetterEvent` is simply the first type to hit it, because the store registers it eagerly. Your
own document types hit exactly the same wall.

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
`Polecat.AotRuntimeSmoke` is. #733 tracks the remaining failure.

## How far it gets today

Measured, not estimated — `Polecat.AotRuntimeSmoke` publishes native and runs against a real SQL
Server. **18 shapes, 16 of which pass.** The two that do not are a projection into an anonymous type
(#743) and a strong-typed document id (#733); everything else below works.

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
| a projection into an **anonymous type** | ❌ [#743](https://github.com/JasperFx/polecat/issues/743) |
| a **strong-typed** document id (`readonly record struct FooId(Guid)`) | ❌ [#733](https://github.com/JasperFx/polecat/issues/733) |

### What makes the supported shapes work

Everywhere `TDoc` is already a type parameter and only the *id* type was closed at runtime, the four
canonical id types are now closed **statically** — `BuildTypedProvider<TDoc, Guid>` and its three
siblings are ordinary generic calls ILC can see and compile. The id type is still decided at runtime;
what changed is that the generic is no longer *closed* at runtime.

`MakeGenericType` / `MakeGenericMethod` can close an instantiation whose arguments are all
**reference** types, because those share one canonical body — so reaching these methods from a
runtime `Type` is fine. It is a **value-type** argument that has no compiled code, and an id type is
routinely `Guid`, `int` or `long`.

⚠️ A **strong-typed id** wrapper is still closed reflectively, because its type is a runtime value
nothing in Polecat can name. A natively-published store whose documents use `readonly record struct`
ids will still fail; plain `Guid` / `string` / `int` / `long` ids work.

### LINQ works, and the one shape that does not is a projection into an anonymous type

`Query<T>()` reads, enum comparisons, child-collection filters, the scalar aggregates (`CountAsync`,
`AnyAsync`, `SumAsync`, `MinAsync`, `AverageAsync`), `GroupBy` and `GroupJoin(...).SelectMany(...)`
all run in a native image, measured by `Polecat.AotRuntimeSmoke`.

::: danger A projection into an anonymous type cannot work
```cs
// ❌ fails in a native image
.GroupBy(q => q.Difficulty).Select(g => new { g.Key, Count = g.Count() })

// ✅ identical query, named projection — works
.GroupBy(q => q.Difficulty).Select(g => new Tally { Key = g.Key, Count = g.Count() })
```

`GroupByListHandler` deserializes the projection through `System.Text.Json`, which under Native AOT
needs a `JsonTypeInfo` from your source-generated context. **You cannot supply one for an anonymous
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
`[RequiresDynamicCode]` to be narrowed to "the shapes that genuinely need a JIT". Measuring found
that set is **empty**: `MakeGenericType` on these paths closes over reference types, which share a
canonical body, and ILC interprets the expression trees `WhereClauseParser` compiles rather than
refusing them.

The annotation stays anyway, and the reasoning is worth stating because it is the opposite of what
the measurement first suggested. Removing a warning asserts safety across the **whole** surface —
17 files in `Polecat/Linq` carry it, covering includes, metadata, soft deletes, cursor paging and
the selectors, and the measured shapes are a fraction of that. Removing it would also mean putting a
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

### What does not work: a strong-typed document id

```
NotSupportedException: 'ValueTypeIdentification`3[Badge,BadgeId,System.Guid]'
is missing native code or metadata.
```

`BuildValueTypeProvider` closes that generic over the *wrapper* and its *inner* type, both value
types, and the wrapper is a runtime value nothing in Polecat can name. Plain `Guid` / `string` /
`int` / `long` ids are fine. Tracked on [#733](https://github.com/JasperFx/polecat/issues/733).

## The consumer contract

⚠️ Two of the walls found on the way turned out **not** to be Polecat defects, and they will still
apply once #733 closes. If you publish natively you have to do both.

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

### Do not use `InvariantGlobalization`

```
NotSupportedException: Globalization Invariant Mode is not supported.
```

The event-store paths need real globalization data, so `<InvariantGlobalization>true</InvariantGlobalization>`
breaks event append and aggregation. It is a tempting switch in an AOT project because it shrinks the
image; it is not available here.

## If you need AOT today

You have a working subset, which is new: meet the three contract items above and a natively-published
app can construct a store, migrate its schema, write documents, load them by id, append and read
events, and aggregate a stream live. What you cannot do is use `Query<T>()` or a strong-typed
document id.

Measured rather than asserted — `Polecat.AotRuntimeSmoke` publishes native and runs every one of
those shapes against SQL Server in CI. Follow
[#733](https://github.com/JasperFx/polecat/issues/733) for the rest.

## How this is tested

Two CI lanes, with deliberately different jobs:

| lane | what it does | what it proves |
|---|---|---|
| `aot-smoke` | **builds** a static-mode consumer with IL2026/IL3050 promoted to errors | the AOT-*clean* surface has not regressed its annotations |
| `aot-runtime-smoke` | **publishes native and runs** against SQL Server | what actually happens, shape by shape |

`aot-runtime-smoke` is `continue-on-error` while #733 and the anonymous-projection shape (#743) are
open, so it reports **two** known failures. A build-only lane cannot catch a runtime AOT failure,
which is why the second one exists — and `PolecatQueryableExtensions` suppressing its own IL3050 is
exactly why `aot-smoke` is silent on the path that broke.

⚠️ Read the lane's failures before believing them. Two of the shapes added in #743 failed on their
first run for reasons that were the **harness's** fault, not Polecat's: one query was simply
unsupported (`GroupJoin` without a following `SelectMany`, which Polecat already refuses clearly),
and one projection type was missing from the smoke's own `JsonSerializerContext`. Only after fixing
both did the real limitation — that an anonymous projection *cannot* be registered — separate out.
