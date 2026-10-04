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

## If you need AOT today

You do not have a working option within Polecat, and this page would rather say so than suggest a
workaround that also throws. Raw SQL through `session.QueryAsync<T>` avoids building an expression
tree, but it still goes through `DocumentMapping`, so it fails at store construction like everything
else.

Follow [#733](https://github.com/JasperFx/polecat/issues/733). The realistic fix is
source-generating the per-document-type plumbing in the consumer's assembly, the way
`JasperFx.Events.SourceGenerator` already handles projection dispatch.

## How this is tested

Two CI lanes, with deliberately different jobs:

| lane | what it does | what it proves |
|---|---|---|
| `aot-smoke` | **builds** a static-mode consumer with IL2026/IL3050 promoted to errors | the AOT-*clean* surface has not regressed its annotations |
| `aot-runtime-smoke` | **publishes native and runs** against SQL Server | what actually happens — currently #733, on every shape |

`aot-runtime-smoke` is `continue-on-error` while #733 is open. A build-only lane cannot catch a
runtime AOT failure, which is why the second one exists.
