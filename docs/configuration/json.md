# JSON Serialization

Polecat uses **System.Text.Json** exclusively for all JSON serialization. Newtonsoft.Json is not supported.

## Default Configuration

Out of the box, Polecat uses camelCase property naming:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // These are the defaults:
    opts.Serializer(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
});
```

## Serializer Options

Configure serialization behavior through `StoreOptions`:

### Enum Storage

```cs
// Store enums as integers (default)
opts.UseSystemTextJsonSerializerOptions(o =>
{
    o.Converters.Add(new JsonStringEnumConverter()); // Store as strings instead
});
```

### Casing

```cs
// CamelCase (default) - "firstName"
opts.Serializer(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});

// Keep original casing - "FirstName"
opts.Serializer(new JsonSerializerOptions
{
    PropertyNamingPolicy = null
});
```

### Non-Public Members

```cs
// Include non-public properties in serialization
opts.UseSystemTextJsonSerializerOptions(o =>
{
    o.IncludeFields = true;
});
```

## SQL Server 2025 JSON Type

Polecat stores all document bodies and event data using SQL Server 2025's native `json` data type by default. This provides:

- Native JSON validation at the database level
- Efficient JSON path queries via `JSON_VALUE()` and `JSON_QUERY()`
- `JSON_MODIFY()` for partial updates (used by the [patching API](/documents/partial-updates-patching))
- Smaller storage footprint compared to `nvarchar(max)`

::: tip
The `json` type in SQL Server 2025 is analogous to PostgreSQL's `jsonb` type used by Marten, but without the binary storage optimization.
:::

### Falling Back to nvarchar(max)

If you are running against a pre-2025 SQL Server instance that does not support the native `json` data type, disable it with:

```cs
opts.UseNativeJsonType = false;
```

When set to `false`, Polecat uses `nvarchar(max)` for all JSON columns instead. All JSON querying, patching, and projection features continue to work with either column type, with the one difference in what numbers each can hold, below.

### Numbers in the `json` Type

The native `json` type doesn't store every number it is given. It holds a number in at most 38 digits, and left to itself it changes one it can't hold without raising an error:

| Written as | Stored by the `json` type |
|---|---|
| `1.2345678901234567E-05` | `0.0000123457` |
| `1.5E-30` | `0.0000000000` |
| `1E+28` | `9999999999999999583119736832.0000000000` |
| `0.123456789012345678901234567890123456789` (39 digits) | `0.1234567890` |
| `1.5E+30`, `1E+40`, `double.MaxValue` | Refused with Msg 1007 |

System.Text.Json writes a `double` with an exponent from around `1E+16` upwards and below `0.0001`, so ordinary values are exposed to this, not just extreme ones. `nvarchar(max)` stores text and has none of these limits.

So that a stored value is the value you saved, Polecat writes every number in full before it reaches a `json` column. This applies to documents, event data, event and document headers, and patches, whichever serializer produced the JSON. A `double` of `1.5E-30` is stored as `0.0000000000000000000000000000015`, comes back exactly, and compares correctly in LINQ.

A number fits when its digits total at most 38, counting the zeros between the decimal point and the first digit of a small number:

| Value | Fits? |
|---|---|
| Anything below `1E+38` with no more digits than a `double` has | Yes, down to around `1E-21` at full precision |
| `1.5E-30` | Yes, 31 decimal places |
| `1E-38` | Yes, exactly 38 decimal places |
| `1E+38`, `double.MaxValue`, `float.MaxValue` | No |
| `6.62607015E-34` | No, 42 decimal places |
| `1.2345678901234567E-23` | No, 39 decimal places |
| `double.Epsilon` | No |

A number that doesn't fit is refused: `SaveChangesAsync()`, or the patch call itself, throws `NativeJsonNumberException`, which names the number and the member, and nothing is saved. There are two ways to keep such a value:

```cs
// Write this member as a JSON string. Its value still compares and sorts in LINQ.
[JsonNumberHandling(JsonNumberHandling.WriteAsString | JsonNumberHandling.AllowReadingFromString)]
public double Largest { get; set; }

// Or keep all JSON as text
opts.UseNativeJsonType = false;
```

::: warning
This protects what is saved from now on. A number that was already changed by the server before this check existed can't be recovered from the database.
:::

A `-0` double is stored as `0`.
