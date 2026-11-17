# Personal Data and GDPR Erasure

An event store keeps everything by design. A request under GDPR Article 17, the right to erasure,
asks for the opposite. The usual answer in an event-sourced system is **crypto-shredding**:

- personal data is encrypted with a key that belongs to one data subject, before it is stored;
- erasing that person means deleting their key;
- the ciphertext can stay where it is, because nothing can read it any more.

Events stay immutable and streams stay intact. The data is unreadable everywhere a copy of it went,
including projections, messages and backups, without anything having to find those copies.

## Tayra is the Critter Stack's answer

[Tayra](https://tayra.dev) is the officially sanctioned crypto-shredding solution for the Critter
Stack, from Radarleaf Technologies and Babu Annamalai, a Critter Stack co-maintainer. It provides:

- **field-level encryption** of personal data;
- **HMAC blind indexes**, so encrypted fields can still be queried by exact value;
- **GDPR Article 17 erasure** by deleting a data subject's key.

Its documentation is at [docs.tayra.dev](https://docs.tayra.dev).

::: warning
**Today Tayra's Critter Stack integration covers Marten and Wolverine, not Polecat.**

- Tayra.Marten covers documents and events, inline, live and async projections, and multi-tenancy.
- Tayra.Wolverine covers message bodies, the outbox, the inbox and dead letters.

So a Wolverine application on Polecat gets Tayra's coverage for its messages. Polecat's own documents
and events have no Tayra integration yet. Check [docs.tayra.dev](https://docs.tayra.dev) for the
current list before planning around it.
:::

Polecat does not ship a crypto-shredding mechanism of its own, and will not. Tayra is the answer, and
a second, Polecat-only mechanism would be one that application code could not carry to Marten or
Fisher.

## What Polecat has built in, and why none of it is erasure

Polecat has several operations that remove or hide event data. Each answers a different question, and
none of them reaches every copy of the data.

| Operation | What it does | What it does not reach |
| :--- | :--- | :--- |
| [Archiving](/events/archiving) | Marks the stream no longer live | Anything. The events are all still there. |
| [Compacting](/events/archiving#compacting-a-stream) | Replaces the history with one `Compacted<T>` event | The snapshot itself, which carries the aggregate's state and so any personal data in it. Also anything an `Archiver` copied out first. |
| Event data masking (`ApplyEventDataMasking`) | Rewrites protected fields in stored event bodies | Projected documents and snapshots already built from the old bodies |
| [`TombstoneStream`](/events/archiving#tombstoning-deleting-a-stream) | Hard-deletes the stream and its events | Projected documents built from those events |

Two more gaps apply to every row in the table:

- **Backups.** A database backup, and every transaction log backup taken since the data was written,
  is a copy of the data. Nothing done to the live database reaches it.
- **The database files.** SQL Server does not overwrite a row the moment it is deleted or updated. The
  old content can stay in the data file until the space is reused, and it is in the transaction log
  until that is truncated and overwritten.

Crypto-shredding avoids both gaps, because it removes the key rather than chasing every copy of the
data.

## If you mask instead

Event data masking is still the right tool for a narrower job: removing information you should never
have stored, from the live database. If you use it as part of an erasure process:

- **rebuild every projection that read the masked events**, because masking leaves derived state
  alone;
- **decide what happens to backups** separately, since masking cannot reach them.
