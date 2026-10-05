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

::: tip This changes nothing for a normal application
IL2026 and IL3050 are only reported when you are trimming or AOT-publishing. If you are not, you see
no new warnings.
:::

The annotation propagates, so these now carry it too: `ToPagedListAsync`, `AggregateToAsync`,
`HybridSearchAsync` / `HybridSearchWithScoresAsync` (its WebStyle text leg executes a LINQ query).

## What is *not* yet annotated

⚠️ **Clean compilation is not a promise of AOT safety anywhere in Polecat.** The keyed load path
(`LoadAsync`), event reads (`FetchStreamAsync`, `AggregateStreamAsync`) and document writes are all
equally broken under AOT today, and simply have no annotation saying so. #733 is the tracking issue
for the whole surface; this release annotated the one that was actively *asserting* its own safety.

## How far it gets today

Measured, not estimated — `Polecat.AotRuntimeSmoke` publishes native and runs. As of this release the
store **builds, connects, and migrates its schema** under Native AOT; it fails when it tries to
construct a document provider.

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

### LINQ works — and the warnings are now over-broad

`Query<T>()` reads, enum comparisons and child-collection filters all run in a native image,
measured by `Polecat.AotRuntimeSmoke`.

⚠️ The LINQ async surface still carries `[RequiresDynamicCode]` from
[#738](https://github.com/JasperFx/polecat/pull/738), which was honest when LINQ genuinely threw and
is now **too broad**. Narrowing it is tracked on
[#741](https://github.com/JasperFx/polecat/issues/741). Marten documents two shapes that genuinely
need a JIT and are worth avoiding here too: a compiled query with an `enum`-typed parameter, and a
`where` clause whose value comes from a **method call** — hoist that into a local first.

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
| `aot-runtime-smoke` | **publishes native and runs** against SQL Server | what actually happens — currently #733, on every shape |

`aot-runtime-smoke` is `continue-on-error` while #733 is open. A build-only lane cannot catch a
runtime AOT failure, which is why the second one exists.
