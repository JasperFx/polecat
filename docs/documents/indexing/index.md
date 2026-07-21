# Indexing Documents

::: warning
Polecat owns the indexes on its own tables, so any custom index needs to be declared through Polecat
itself. An index created out of band may be dropped the next time Polecat reconciles the schema.
:::

Like Marten, Polecat gives you several ways to speed up document queries — all of which trade slightly
slower inserts for faster reads. Polecat supports:

* [Computed indexes](/documents/indexing/computed-indexes) — persisted computed columns backed by
  `JSON_VALUE`, with standard nonclustered indexes. Supports composite keys, uniqueness, case
  transformations, filtered (partial) indexes, per-tenant scoping, and covering `INCLUDE` columns.
* [JSON indexes](/documents/indexing/json-indexes) — native SQL Server 2025 `CREATE JSON INDEX` over the
  `data` column, covering many JSON paths with one index (the SQL Server counterpart to Marten's
  `GinIndexJsonData`).

Indexes can be declared with the fluent `Schema.For<T>()` API or, for computed indexes, with `[Index]` /
`[UniqueIndex]` attributes on the document type itself. As in Marten, the choice between keeping
persistence concerns off your document types (fluent API) and colocating them for traceability
(attributes) is yours — both are supported and can be combined on the same type.
