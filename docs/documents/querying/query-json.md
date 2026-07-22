# Querying for Raw JSON

Polecat can return documents as raw JSON strings without deserializing them into .NET objects.

## LoadJsonAsync

Load a single document as JSON by ID:

```cs
string? json = await session.LoadJsonAsync<User>(userId);
```

Returns `null` if the document doesn't exist.

## ToJsonArrayAsync

Convert a LINQ query result to a JSON array string:

```cs
string jsonArray = await session.Query<User>()
    .Where(x => x.Active)
    .ToJsonArrayAsync();

// Returns: [{"id":"...","firstName":"Alice",...},{"id":"...","firstName":"Bob",...}]
```

### Projecting with Select()

A **simple** `Select()` projection — an anonymous type or DTO composed only of (optionally
nested) document member accesses — is translated to a server-side JSON object and streamed
without hydrating the documents (SQL Server 2025 native JSON, via `JSON_OBJECT`):

```cs
string jsonArray = await session.Query<Customer>()
    .Where(x => x.Active)
    .Select(x => new { x.Name, City = x.Address.City })
    .ToJsonArrayAsync();

// Returns: [{"name":"Alice","city":"Austin"}, ...]
```

Emitted keys honor the serializer naming policy and `[JsonPropertyName]`, exactly as
System.Text.Json would serialize the projected shape. Safe/widening conversions (`int`→`long`,
`T`→`object`, enum↔underlying, non-null→nullable) are transparent.

::: warning
A projection that is **not** a simple member projection — a scalar select (`Select(x => x.Name)`),
a method call (`x.Name.ToUpper()`), arithmetic (`x.Age * 2`), a conditional, etc. — **cannot** be
streamed as raw JSON and throws `BadLinqExpressionException` rather than silently returning the raw
documents. Materialize those with `ToListAsync()` (which runs the projection client-side) instead
of a JSON-streaming call. The server-side JSON-object translation requires a native-JSON
(SQL Server 2025) store; on other stores a `Select()` projection likewise throws when streaming.
:::

## Use Cases

Raw JSON queries are useful when:

- Serving JSON directly to HTTP responses without deserialization/serialization overhead
- Building APIs where the response format matches the stored document
- Streaming large result sets to clients
- Debugging or inspecting stored data
