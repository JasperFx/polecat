# DCB load test: Polecat 6.0 (#972, #973, #975)

Measured 2026-11-14 on branch `dcb-append-condition-compliance` with this harness, in Release on .NET 10.0.14, on an 18-core Apple-silicon Mac. The database was `mcr.microsoft.com/mssql/server:2025-latest` (SQL Server 17.0.4045.5 Developer) in Docker, with RCSI off, snapshot isolation off, ADR off and compatibility level 170, in a catalog of its own (`polecat_dcb_load`). Nothing else ran against the server.

This is a port of Marten's 10.0 harness (JasperFx/marten#5899, `RESULTS-10.0.md`), and the scenarios have the same shapes so the two can be read side by side. **Read the ratios between configurations, not the absolute rates.** The SQL Server image is x86_64 only, so on this machine it runs under Rosetta emulation. Every absolute number is several times what native hardware would give, and it is not comparable with Marten's numbers from native PostgreSQL.

Two harness columns don't mean what they mean in Marten's:

- **Write retries.** Polecat's default `ResiliencePipeline` has no retry strategy (#724), so "on" and "off" are the same configuration. Only "off" (`ConfigurePolly(_ => { })`) was run, and a deadlock would have surfaced as an error rather than being retried away.
- **Version-row writes.** Polecat's check reads a row it only checks `WITH (UPDLOCK, ROWLOCK)` rather than doing a no-op update, so the check is not counted as a write. Under `TagValueAndEventType` a decision writes 2.00 rows here against Marten's 2.86.

## Recommendation

1. **Keep `TagValueAndEventType` as the 6.0 default.** On the #5887 reporter's workload it is the difference between 64/250 and 250/250 requests succeeding, with zero conflicts and 6.7x the commit rate. Its costs measured at noise level, apart from table size:
   - tagged-append throughput within -4% to +3% of `TagValue`, while writing 2-3x the version rows;
   - the same steady-state decision throughput in the 300 s soak (1,349-1,377 against 1,387-1,423 decisions/s);
   - `pc_dcb_tag_version` 2-3x larger: 19,873 rows / 2.5 MB against 7,633 / 0.9 MB after the append runs, and 574k rows / 71 MB against 200k / 22 MB in the soak.
2. **No deadlocks and no lock escalations anywhere.** That held in every configuration, including the crossed probe built to provoke them: 0 on the server's `Number of Deadlocks/sec` counter, 0 client error 1205, 0 `xml_deadlock_report` events in `system_health`, and 0 `index_lock_promotion_count` on the version table. The single sorted lock pass holds on SQL Server. Each check is its own statement on one key, so the 5,000-locks-per-statement escalation threshold is never approached.
3. **Don't port `DcbTagVersionFillFactor`.** In Postgres it buys HOT updates. On SQL Server a version update is in place whatever the fill factor, and fragmentation doesn't slow single-key seeks. The soak ran at 1,387 / 1,393 / 1,423 decisions/s for the default / 100 / 70, which is noise.
4. **Neither of Marten's scans exists in Polecat.**
   - **marten#5897**, the two-tag-type full scan: "customer OR region" at 1M events fetches 1,010 events in 5.6 ms (p50), against Marten's 327 ms. Its cost tracks the events read, not the store's size. `TagConditionSql` already uses the per-(tag type, event type) `UNION` shape.
   - **marten#5896**, a tag write finding its event by the unindexed `id`: every tag-write path joins tags to events on `seq_id`. Quick append reads the new `seq_id` back from the batch's own table variable, the existence check is on the tag table's `(value, seq_id)` key, and archive/tombstone reach events through `stream_id`. Single-tag decisions are flat from 10k to 1M events (3.1 ms to 3.0-3.1 ms).
   - An event tagged with two queried values folds once: see `dcb_tag_read_tests.a_doubly_tagged_event_folds_once_on_every_path`.

## 1. Tagged-append throughput (`append`)

`dotnet run -c Release --project src/DcbLoadTest -- append --single-saves 3000 --batch-saves 600 --concurrent-saves 200`, 3 repeats, median shown. "Version-row writes/save" is the `leaf_insert_count + leaf_update_count` delta on `pc_dcb_tag_version`'s clustered index.

| Variant | Scope | saves/s (min-max) | p50 ms | p99 ms | version-row writes/save | row lock waits/save | vs TagValue |
|---|---|---|---|---|---|---|---|
| 1 event, 1 tag, sequential | TagValue | 322 (315-324) | 2.99 | 4.83 | 1.0 | 0.00 | |
| | TagValueAndEventType | 323 (315-344) | 2.98 | 4.44 | 2.0 | 0.00 | +0% |
| 10 events (2 types), customer + region tags, sequential | TagValue | 195 (193-203) | 4.52 | 7.16 | 2.0 | 0.00 | |
| | TagValueAndEventType | 188 (188-197) | 4.58 | 12.4 | 6.0 | 0.00 | -4% |
| same, untagged (baseline) | - | 307-316 | 2.96-3.11 | 4.5 | 0 | 0.00 | |
| same, 16 writers, region 1 of 10 (hot version rows) | TagValue | 637 (636-646) | 18.6 | 69.7 | 2.0 | 0.41 | |
| | TagValueAndEventType | 650 (638-659) | 20.5 | 67.9 | 6.0 | 0.41 | +2% |

- **The per-event-type versions triple the version-row writes and cost no measurable throughput**, even with 16 writers contending on 10 hot region rows, where the extra rows are locked inside the same sorted pass and lock waits per save are identical.
- Tagging ten events with two tags costs about 35% of the untagged rate in this environment, under either scope.

## 2. `FetchForWritingByTags` latency (`read`)

`read --seed-streams 1000` and `--seed-streams 100000`: 10 events per customer, alternating `OrderPlaced`/`OrderShipped`, each also tagged with 1 of 1,000 regions. Seeded under `TagValueAndEventType`, so both kinds of version row exist. 1,000 sequential iterations per row, random customer. "Decision" is fetch + `AppendOne` + `SaveChangesAsync`, uncontended.

| Store | Scope | Query | events | fetch p50 | fetch p99 | decision p50 | decision p99 |
|---|---|---|---|---|---|---|---|
| 10k | TagValue | 1 tag | 10 | 0.66 | 0.84 | 3.17 | 4.64 |
| 10k | TagValueAndEventType | 1 tag | ~13 | 0.62 | 0.82 | 3.18 | 4.34 |
| 10k | either | 1 tag + AndEventsOfType | 5 | 0.59 | 0.77-0.79 | 3.09-3.21 | 4.0-4.4 |
| 10k | either | no events yet | 0 | 0.50-0.51 | 0.67-0.69 | 3.10 | 3.9-5.2 |
| 10k | either | customer OR region | 22-25 | 0.70-0.71 | 0.89-0.91 | 3.23-3.27 | 4.4-4.5 |
| **1M** | TagValue | 1 tag | 10 | 0.65 | 1.19 | 3.02 | 4.23 |
| **1M** | TagValueAndEventType | 1 tag | 10 | 0.61 | 0.82 | 3.06 | 3.89 |
| **1M** | either | 1 tag + AndEventsOfType | 5 | 0.59-0.60 | 0.74-0.83 | 3.09-3.61 | 4.1-7.7 |
| **1M** | either | no events yet | 0 | 0.50-0.51 | 0.64-0.65 | 3.00-3.13 | 4.9-6.0 |
| **1M** | either | customer OR region | 1,010 | **5.56-5.60** | 6.7-7.7 | 8.42-8.50 | 9.6-12.7 |

- **The scope does not change read latency.** The version capture is one primary-key seek per distinct key either way.
- **Single-tag reads and decisions are flat from 10k to 1M events.**
- **Two tag types scale with the events read** (22 → 1,010 events, 0.7 → 5.6 ms), not the store. Marten's #5897 does not apply.
- A decision costs about 2.5 ms beyond the fetch. Most of that is round trips to an emulated server, and it bounds the contended commit rate in section 3.

## 3. Contended decisions (`contend`)

`contend --write-retries off`: 50 requests released together on one class, 5 rounds (250 requests). Each request fetches the class boundary (`class AND (ClassScheduled, SpotBooked)`), appends one event and saves, retrying `DcbConcurrencyException` up to 2 more times with no wait. `book` is the share of requests that append `SpotBooked`, which everyone's query matches, while seats remain (capacity 100); the rest append `WaitlistJoined`, which nobody's query matches. "check" verifies booked ≤ capacity and events appended = successful requests.

| book | Scope | succeeded | DcbConcurrencyException | 1st-attempt conflicts | commits/s | request p50 / p99 ms | save p50 / p99 ms | deadlocks | check |
|---|---|---|---|---|---|---|---|---|---|
| 0 | TagValue | 64/250 | 628 | 94.0% | 28 | 187 / 630 | 5.3 / 130 | 0 / 0 / 0 | ok |
| 0 | TagValueAndEventType | **250/250** | **0** | 0% | **188** | 93 / 460 | 37.3 / 313 | 0 / 0 / 0 | ok |
| 0.2 | TagValue | 44/250 | 668 | 96.0% | 30 | 176 / 330 | 4.9 / 136 | 0 / 0 / 0 | ok |
| 0.2 | TagValueAndEventType | 97/250 | 556 | 88.0% | 85 | 212 / 271 | 20.5 / 162 | 0 / 0 / 0 | ok |
| 1 | TagValue | 26/250 | 707 | 98.0% | 97 | 45 / 59 | 7.9 / 14.2 | 0 / 0 / 0 | ok |
| 1 | TagValueAndEventType | 20/250 | 709 | 97.2% | 39 | 85 / 134 | 14.8 / 36.5 | 0 / 0 / 0 | ok |

Deadlocks: server counter delta / client 1205 / `system_health` reports naming the database.

- **#973 is fixed as reported.** Under `TagValue`, waitlist joins knock each other out exactly as in marten#5887 (64/250). Under `TagValueAndEventType` they no longer conflict at all.
- **Correct conflicts still fire.** When everyone books, both scopes conflict 97-98% of the time on the first attempt, and the overbooking and replay check never failed.
- **The serialization cost is real and bounded.** Non-conflicting decisions on one class still queue on its version rows: each one bumps the class's tag-value row, which every append carrying the tag must do. That's why the save p50 rises to ~37 ms when 50 commit back to back, and it puts a ceiling of ~190 commits/s on one hot tag value in this environment. That is about 5 ms per commit, twice an uncontended save.
- **Fewer round trips under the lock didn't help.** I tried sending the DCB phase in the same batch as the stream-state read, which saves one round trip while the row locks are held. It measured 167-188 commits/s on the same workload, and uncontended decisions stayed at 3.1-3.3 ms, so I reverted it. The round trip isn't where the time goes.
- **Retries made no difference to correctness,** because there were no deadlocks to absorb.

### 3b. Crossed boundaries (`crossed`)

`crossed --write-retries off`: 50 concurrent decisions over 4 classes, 5 rounds. Each decision reads class X and appends to class Y ≠ X, and half the appends also carry X. So concurrent saves check X and bump Y while others check Y and bump X.

| Scope | succeeded | DcbConcurrencyException | commits/s | save p50 / p99 ms | deadlocks | lock escalations | check |
|---|---|---|---|---|---|---|---|
| TagValue | 193/250 | 356 | 75 | 8.8 / 151 | 0 / 0 / 0 | 0 | ok |
| TagValueAndEventType | 204/250 | 305 | 93 | 18.2 / 179 | 0 / 0 / 0 | 0 | ok |

No deadlocks in the crossed shape either. It is also pinned by the compliance suite's `crossed_boundaries_lose_with_a_dcb_conflict_not_a_deadlock` and `decisions_crossed_by_event_type_lose_with_a_dcb_conflict_not_a_deadlock`, which run with retries off.

## 4. `pc_dcb_tag_version` growth (`growth`)

`growth --fillfactors none,100,70`: 200,000 customers, each pre-created with one `OrderPlaced`. Then 16 writers run for 300 s; each decision reads `customer AND OrderPlaced` for a uniformly random customer and appends `OrderShipped`. "fillfactor" rebuilds the version table's clustered index with that fill factor after the pre-create, which the harness does itself (Polecat has no option for it). In place of Postgres's HOT ratio it reports page splits (`leaf_allocation_count`), fragmentation and ghost records.

| Scope | fillfactor | saves | saves/s | p50 / p99 ms | conflicts | version rows start → end | row writes/save | page splits | reserved MB | page fullness % | fragmentation % | ghosts at end |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| TagValue | default | 416,197 | 1,387 | 11.0 / 20.0 | 8 | 200k → 200k | 1.00 | 0 | 22.0 → 22.0 | 69.3 | 99.5 → 99.5 | 0 |
| TagValue | 100 | 417,820 | 1,393 | 11.0 / 19.6 | 11 | 200k → 200k | 1.00 | 0 | 15.4 → 15.4 | 99.2 | 0.0 → 0.0 | 0 |
| TagValue | 70 | 426,940 | 1,423 | 10.8 / 19.0 | 11 | 200k → 200k | 1.00 | 0 | 21.7 → 21.7 | 70.3 | 0.0 → 0.0 | 0 |
| TagValueAndEventType | default | 404,669 | 1,349 | 11.3 / 20.3 | 0 | 400k → 574k | 2.00 | 2,869 | 48.6 → 71.3 | 68.5 | 99.3 → 99.3 | 0 |
| TagValueAndEventType | 100 | 413,019 | 1,377 | 11.1 / 20.3 | 0 | 400k → 575k | 2.00 | 2,874 | 33.5 → 56.2 | 99.0 → 86.9 | 0.0 → 40.3 | 0 |
| TagValueAndEventType | 70 | 413,182 | 1,377 | 11.1 / 20.7 | 0 | 400k → 575k | 2.00 | 2,845 | 46.8 → 69.4 | 70.6 → 70.3 | 0.0 → 32.3 | 0 |

- **Version updates are in place.** Under `TagValue`, 416k updates caused no page splits and left no ghost records, at any fill factor.
- **`TagValueAndEventType`'s growth is new rows, not bloat.** The `(customer, OrderShipped)` rows appear as each customer ships for the first time (400k → 574k). Those inserts are the page splits.
- **The table is ~99% fragmented without a rebuild,** because tag values are random GUIDs inserted all over the clustered key. It doesn't matter: every access is a single-key seek, and throughput was the same at every fill factor. A rebuild only saves space (22 → 15 MB at fill factor 100).
- **Conflicts are correct.** Under `TagValue`, two random decisions on the same customer conflict even though neither appends what the other reads (8-11 in ~420k). Under `TagValueAndEventType` none do.

## Findings outside DCB

- `StoreOptions.Connection(SqlConnection)` copies `connection.ConnectionString`, which SqlClient strips of the password once the connection has been opened (unless `Persist Security Info=True`), and which never carries a `SqlCredential` or access token. A freshly resolved Aspire connection is fine, but an already-opened one gives the store a string it can't log in with. It's the same failure shape as marten#5898, from a different cause.

## Harness

`src/DcbLoadTest`: `append | read | contend | crossed | growth | legacy` (`legacy` is the original append benchmark). See `Program.cs` for the options. It needs its own catalog, `polecat_dcb_load` by default, created if missing; override the connection with `POLECAT_DCB_LOAD_DATABASE`. Statistics are read as deltas, and the deadlock counter is instance-wide, so nothing else should run on the server. `system_health` reports are dispatched lazily, so each scenario re-reads them at its end.
