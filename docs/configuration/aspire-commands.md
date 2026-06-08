# JasperFx Commands in the Aspire Dashboard <Badge type="tip" text="4.2" />

The optional **`JasperFx.Aspire`** package surfaces a Polecat application's
command-line verbs as clickable **custom commands** on each resource tile in the
[.NET Aspire dashboard](https://learn.microsoft.com/en-us/dotnet/aspire/fundamentals/dashboard).
With one extension call in your AppHost project, an operator running the Aspire
dashboard against a local or staging environment can run **`check-env`**,
**`describe`**, **`resources`**, or **`projections`** against a live Polecat
service without dropping to a terminal — output streams back into the
dashboard's resource console.

Polecat apps inherit this for free because they build on the same JasperFx
command layer that Marten and Wolverine use. Because the projection commands are
defined on the shared `JasperFx.Events` abstractions, the `projections` and
`resources` buttons behave identically whether your event store is backed by
SQL Server (Polecat) or PostgreSQL (Marten). See the
[JasperFx.Aspire package README](https://github.com/JasperFx/jasperfx/tree/master/src/JasperFx.Aspire)
for the full, store-agnostic reference.

## Quick start

Add the package to your **Aspire AppHost** project (not the Polecat service
project itself):

```shell
dotnet add package JasperFx.Aspire
```

Then opt in on the Polecat service resource:

```csharp
using JasperFx.Aspire;

var builder = DistributedApplication.CreateBuilder(args);

builder.AddProject<Projects.PolecatApi>("api")
    .WithJasperFxCommands();

builder.Build().Run();
```

That adds the **safe-by-default** command buttons — `check-env`, `describe`, and
`codegen` (preview only) — to the `api` resource tile. Click any of them in the
dashboard, the verb runs against the live service with the same environment
Aspire injects, and the output streams into the resource's console log view.

This works because the target Polecat service already ends its `Program.cs` with
`RunJasperFxCommands(args)` (the standard JasperFx bootstrap) — the integration
reuses that exact CLI path, so the service itself needs no changes.

## The verbs that matter for Polecat users

- **`check-env`** *(read-only)* — runs every registered environment check.
  Confirms the Polecat service can reach its SQL Server database, that all
  projection dependencies are wired, that the required schemas exist, etc.
- **`describe`** *(read-only)* — dumps the resolved Polecat `StoreOptions`
  (document mappings, event store config, projections, retry policies, tenancy
  strategy, …). Useful for verifying composite configuration at a glance.
- **`resources`** *(mutating)* — applies / patches Polecat's schema objects
  (`pc_events`, `pc_streams`, `pc_event_progression`, document tables, indexes,
  projection tables). Equivalent to applying all configured changes to the
  database.
- **`projections`** *(mutating)* — runs or **rebuilds** asynchronous
  projections. The rebuild path reprocesses the event store — long-running and
  disruptive on a populated store, which is why it prompts for confirmation.

## Opting in to mutating verbs

Mutating verbs are off by default. Adding them is a one-liner:

```csharp
builder.AddProject<Projects.PolecatApi>("api")
    .WithJasperFxCommands(opts =>
    {
        // Adds resources + projections + codegen-write buttons.
        opts.IncludeMutatingCommands = true;
    });
```

When `IncludeMutatingCommands = true`, every mutating verb requires an explicit
**confirmation dialog** in the Aspire dashboard before it runs. The default
confirmation copy is generic ("Run `projections` on `api`?"); customize per-verb
when the impact is non-obvious:

```csharp
builder.AddProject<Projects.PolecatApi>("api")
    .WithJasperFxCommands(opts =>
    {
        opts.IncludeMutatingCommands = true;

        opts.For("projections").ConfirmationMessage =
            "Rebuild ALL projections for 'api'? This reprocesses the entire event store.";

        opts.For("resources").ConfirmationMessage =
            "Apply pending schema changes to the 'api' SQL Server database?";
    });
```

## Per-verb tweaks

`opts.For("verb")` returns a registration object that lets you override the
dashboard presentation per verb:

| Property              | Use                                                                                       |
| --------------------- | ----------------------------------------------------------------------------------------- |
| `DisplayName`         | Button label (defaults to a humanized verb name).                                         |
| `DisplayDescription`  | Tooltip / extended description.                                                            |
| `IconName`            | Fluent UI icon name; sensible defaults per verb.                                          |
| `ConfirmationMessage` | Required for mutating verbs; setting this opts a non-mutating verb into confirmation too. |
| `IsHighlighted`       | Pins the button to the front of the strip.                                                |
| `IsEnabled`           | Predicate over the resource's current state — useful for gating verbs to `Running`.       |

## Adding a single verb

When the curated default set isn't quite what you want, register verbs
one-at-a-time with `WithJasperFxCommand` instead of the batch helper:

```csharp
builder.AddProject<Projects.PolecatApi>("api")
    .WithJasperFxCommand("projections", "rebuild MyProjection", registration =>
    {
        registration.DisplayName = "Rebuild MyProjection";
        registration.ConfirmationMessage =
            "Rebuild MyProjection for 'api'? This reprocesses the event store.";
        registration.IsHighlighted = true;
    });
```

The second argument is the verb's fixed argument string — handy for locking a
button down to one specific projection rather than exposing the full
`projections` surface.

## Constraints

- **`JasperFx.Aspire` runs at the AppHost project layer**, not inside the
  Polecat service itself. Adding it as a `<ProjectReference>` of the service
  project is a no-op.
- The verbs run in a **child process** of the Polecat service, with the same
  environment Aspire injects into the resource. If `check-env`, `resources`, or
  `projections` fail to reach Aspire-managed dependencies, verify the dashboard
  shows the resource as `Running` first.
- Buttons require `RunJasperFxCommands(args)` to already be wired in the Polecat
  service's `Program.cs`. Without that wiring the verb spawn succeeds but the
  child process won't recognize the verb.
