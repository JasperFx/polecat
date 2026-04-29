# Introduction

Welcome to the Polecat documentation!

## What is Polecat?

**Polecat is a .NET library for building applications using
a [document-oriented database approach](https://en.wikipedia.org/wiki/Document-oriented_database)
and [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html), backed by SQL Server 2025.**

Polecat is part of the [Critter Stack](https://jasperfx.net) ecosystem and mirrors the API patterns of [Marten](https://martendb.io) (the PostgreSQL equivalent), making it easy for teams already familiar with Marten to adopt SQL Server as their backing store.

::: tip
If you've used Marten before, you'll feel right at home with Polecat. The API surface is intentionally similar -- same interface names, same session patterns, same projection model.
:::

Under the hood, Polecat is built on top of [SQL Server 2025](https://www.microsoft.com/en-us/sql-server/sql-server-2025), leveraging its native JSON type to provide:

- a [document database](/documents/),
- an [event store](/events/).

Polecat uses SQL Server 2025's native `JSON` data type for storing document bodies, event data, and headers. Combined with modern T-SQL features, this provides strong data consistency for both document storage and event sourcing approaches.

## Main Features

| Feature | Description |
| :---: | :---: |
| [Document Storage](/documents/) | Store your entities as JSON documents in SQL Server with full LINQ querying support. |
| [Event Store](/events/) | Full-fledged event store for Event Sourcing with stream management, projections, and subscriptions. |
| [Strong Consistency](/documents/sessions#unit-of-work) | Uses SQL Server transactions for ACID compliance across both document and event operations. |
| [LINQ Querying](/documents/querying/) | Filter documents using LINQ queries with string searching, child collection queries, paging, and more. |
| [Event Projections](/events/projections/) | Build read models from events using inline, async, or live projection strategies. |
| [Automatic Schema Management](/schema/migrations) | Polecat manages SQL Server table creation and migrations automatically via Weasel.SqlServer. |
| [Optimistic Concurrency](/documents/concurrency) | Built-in support for both Guid-based and numeric revision concurrency control. |
| [Multi-Tenancy](/configuration/multitenancy) | Multiple tenancy strategies: conjoined (same tables), separate databases, or single tenant. |
| [Async Daemon](/events/projections/async-daemon) | Background projection processing for eventually consistent read models. |
| [EF Core Integration](/events/projections/efcore) | Use Entity Framework Core DbContext within your event projections. |

## Critter Stack Ecosystem

Polecat is designed to work alongside other Critter Stack libraries:

| Library | Purpose |
| :---: | :---: |
| [Marten](https://martendb.io) | PostgreSQL document database and event store |
| [Wolverine](https://wolverinefx.net) | Messaging and command processing framework |
| [JasperFx](https://jasperfx.net) | Core framework and event sourcing abstractions |
| [Weasel](https://github.com/JasperFx/weasel) | Database schema management |

## Polecat vs Marten

Polecat mirrors Marten's API but targets SQL Server 2025 instead of PostgreSQL:

| Feature | Marten (PostgreSQL) | Polecat (SQL Server) |
| :---: | :---: | :---: |
| JSON storage | `jsonb` type | `json` type (SQL Server 2025) |
| Sequences | `bigserial` / sequences | `bigint IDENTITY(1,1)` |
| Upsert | `INSERT ... ON CONFLICT` | `MERGE` statement |
| Change notification | `LISTEN/NOTIFY` | Polling (configurable interval) |
| Advisory locks | `pg_advisory_lock` | `sp_getapplock` / `sp_releaseapplock` |
| Timestamps | `timestamptz` | `datetimeoffset` |
| Serialization | STJ or Newtonsoft | System.Text.Json only |

## History and Origins

The project name _Polecat_ follows the Critter Stack tradition of naming projects after animals. A [polecat](https://en.wikipedia.org/wiki/Polecat) is a member of the mustelid family -- close relatives of the marten, making it a fitting name for Marten's SQL Server cousin.
