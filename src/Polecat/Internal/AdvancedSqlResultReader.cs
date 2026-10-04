using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using JasperFx;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Internal;

/// <summary>
///     Reads a single result type from a DbDataReader at a given column offset.
///     Supports scalars, JSON-deserialized objects, and full document types.
/// </summary>
internal abstract class AdvancedSqlResultReader
{
    /// <summary>
    ///     Number of columns this reader consumes from the result set.
    /// </summary>
    public abstract int ColumnCount { get; }

    public abstract object? ReadValue(DbDataReader reader, int startColumn);
    public abstract Task<object?> ReadValueAsync(DbDataReader reader, int startColumn, CancellationToken token);

    /// <summary>
    ///     Coerce a value read by <see cref="ReadValue" /> to the caller's requested type.
    /// </summary>
    /// <remarks>
    ///     #676: one definition, shared by the standalone <c>IAdvancedSql</c> overloads and the batched
    ///     raw-SQL item. Tries the direct cast first so a document or JSON object — already the right
    ///     type — passes straight through, and only falls back to <see cref="Convert.ChangeType(object, Type)" />
    ///     for the scalar widening cases (an <c>int</c> column read into a <c>long</c>, say).
    /// </remarks>
    public static T Cast<T>(object? value)
    {
        if (value == null) return default!;
        if (value is T typed) return typed;
        return (T)Convert.ChangeType(value, typeof(T));
    }

    public static AdvancedSqlResultReader ForType(Type type, ISerializer serializer, DocumentProviderRegistry? providers)
    {
        // Check for scalar types first
        if (type == typeof(string)) return new ScalarReader(typeof(string));
        if (type == typeof(int)) return new ScalarReader(typeof(int));
        if (type == typeof(long)) return new ScalarReader(typeof(long));
        if (type == typeof(short)) return new ScalarReader(typeof(short));
        if (type == typeof(byte)) return new ScalarReader(typeof(byte));
        if (type == typeof(bool)) return new ScalarReader(typeof(bool));
        if (type == typeof(decimal)) return new ScalarReader(typeof(decimal));
        if (type == typeof(double)) return new ScalarReader(typeof(double));
        if (type == typeof(float)) return new ScalarReader(typeof(float));
        if (type == typeof(Guid)) return new ScalarReader(typeof(Guid));
        if (type == typeof(DateTime)) return new ScalarReader(typeof(DateTime));
        if (type == typeof(DateTimeOffset)) return new ScalarReader(typeof(DateTimeOffset));

        // Check if it's a known document type (has an Id property and a registered provider)
        if (providers != null)
        {
            try
            {
                var provider = providers.GetProvider(type);
                if (provider != null)
                {
                    return new DocumentReader(type, serializer, provider);
                }
            }
            catch
            {
                // Not a registered document type, fall through to JSON
            }
        }

        // Default: deserialize from a single JSON column
        return new JsonReader(type, serializer);
    }
}

internal class ScalarReader : AdvancedSqlResultReader
{
    private readonly Type _type;

    public ScalarReader(Type type)
    {
        _type = type;
    }

    public override int ColumnCount => 1;

    public override object? ReadValue(DbDataReader reader, int startColumn)
    {
        if (reader.IsDBNull(startColumn)) return null;
        return reader.GetValue(startColumn);
    }

    public override Task<object?> ReadValueAsync(DbDataReader reader, int startColumn, CancellationToken token)
    {
        return Task.FromResult(ReadValue(reader, startColumn));
    }
}

[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: ISerializer.FromJson(Type, string) for advanced SQL projections. Result types flow in from QueryAsync<T>() registration on the caller side and are preserved per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson is annotated RDC. AOT consumers supply a source-generator-backed impl.")]
internal class JsonReader : AdvancedSqlResultReader
{
    private readonly Type _type;
    private readonly ISerializer _serializer;

    public JsonReader(Type type, ISerializer serializer)
    {
        _type = type;
        _serializer = serializer;
    }

    public override int ColumnCount => 1;

    public override object? ReadValue(DbDataReader reader, int startColumn)
    {
        if (reader.IsDBNull(startColumn)) return null;
        var json = reader.GetString(startColumn);
        return _serializer.FromJson(_type, json);
    }

    public override Task<object?> ReadValueAsync(DbDataReader reader, int startColumn, CancellationToken token)
    {
        return Task.FromResult(ReadValue(reader, startColumn));
    }
}

[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: ISerializer.FromJson(Type, string) for advanced SQL document projections. Document types flow in from registration on the caller side and are preserved per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson is annotated RDC. AOT consumers supply a source-generator-backed impl.")]
internal class DocumentReader : AdvancedSqlResultReader
{
    private readonly Type _type;
    private readonly ISerializer _serializer;
    private readonly DocumentProvider _provider;

    public DocumentReader(Type type, ISerializer serializer, DocumentProvider provider)
    {
        _type = type;
        _serializer = serializer;
        _provider = provider;
    }

    // Documents need at minimum: id, data (2 columns)
    // Optionally: version, last_modified, created_at, dotnet_type, tenant_id, guid_version
    public override int ColumnCount => 2; // Minimum: id + data

    public override object? ReadValue(DbDataReader reader, int startColumn)
    {
        if (reader.IsDBNull(startColumn + 1)) return null; // data column is null

        var json = reader.GetString(startColumn + 1); // data is second column
        var doc = _serializer.FromJson(ResolveType(reader), json);

        if (doc == null) return null;

        // Sync metadata if available and extra columns are present
        SyncMetadata(doc, reader, startColumn);

        return doc;
    }

    /// <summary>
    ///     The CLR type to deserialize this row as — the concrete sub-class when the statement
    ///     carried a <c>doc_type</c> column, and the requested type otherwise (#729).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>Found BY NAME rather than by position, which is the whole reason this is
    ///         cheap.</b> The search statements select <c>id, data, &lt;score&gt;</c>, and three
    ///         things read those positions by index that all have to agree:
    ///         <see cref="ColumnCount" />, <c>QueryByBatchAsync&lt;T1, T2&gt;</c> (which reads the
    ///         score at <see cref="ColumnCount" />), and <see cref="SyncMetadata" /> (which probes
    ///         <c>startColumn + 2</c> guarded only by <c>FieldCount</c> and a type test — it is
    ///         already reading the score column on the scored overloads and gets away with it solely
    ///         because a <c>double</c> matches none of its tests). Appending a column and shifting
    ///         any of those indexes is how that becomes a wrong version rather than a compile error.
    ///         A name lookup leaves every index alone.
    ///     </para>
    ///     <para>
    ///         Gated on the mapping being a hierarchy, so a raw-SQL query that happens to select a
    ///         column called <c>doc_type</c> from something unrelated cannot start reinterpreting
    ///         rows.
    ///     </para>
    ///     <para>
    ///         An alias this deployment does not know is data written by one that did, so it falls
    ///         back to the requested type rather than throwing — the same tolerance
    ///         <c>DocumentStore.DocumentDiagnostics</c> already applies to an unknown
    ///         <c>doc_type</c>. A search is a read; refusing the whole page because one row names a
    ///         sub-class deployed elsewhere would be worse than materializing it as its root.
    ///     </para>
    /// </remarks>
    private Type ResolveType(DbDataReader reader)
    {
        var mapping = _provider.Mapping;
        if (!mapping.IsHierarchy()) return _type;

        var ordinal = DocTypeOrdinal(reader);
        if (ordinal < 0 || reader.IsDBNull(ordinal)) return _type;

        try
        {
            return mapping.TypeFor(reader.GetString(ordinal));
        }
        catch (ArgumentOutOfRangeException)
        {
            return _type;
        }
    }

    /// <summary>
    ///     The <c>doc_type</c> column's ordinal, or -1. <see cref="DbDataReader.GetOrdinal" /> throws
    ///     when the column is absent, and absent is the ordinary case here — every non-hierarchy
    ///     statement and every hierarchy statement built before #729 — so the scan is cheaper than
    ///     the exception.
    /// </summary>
    private static int DocTypeOrdinal(DbDataReader reader)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (string.Equals(reader.GetName(i), "doc_type", StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    public override Task<object?> ReadValueAsync(DbDataReader reader, int startColumn, CancellationToken token)
    {
        return Task.FromResult(ReadValue(reader, startColumn));
    }

    private void SyncMetadata(object doc, DbDataReader reader, int startColumn)
    {
        var fieldCount = reader.FieldCount;

        // Try to sync version if the document implements ILongVersioned (long) and there's a 3rd column
        if (startColumn + 2 < fieldCount && doc is ILongVersioned longVersioned)
        {
            try
            {
                var val = reader.GetValue(startColumn + 2);
                if (val is long lv) longVersioned.Version = lv;
                else if (val is int iv) longVersioned.Version = iv;
            }
            catch { /* column type mismatch, skip */ }
        }
        // Try to sync version if the document implements IRevisioned (int) and there's a 3rd column
        else if (startColumn + 2 < fieldCount && doc is IRevisioned revisioned)
        {
            try
            {
                var val = reader.GetValue(startColumn + 2);
                if (val is int intVer) revisioned.Version = intVer;
                else if (val is long longVer) revisioned.Version = (int)longVer;
            }
            catch { /* column type mismatch, skip */ }
        }

        // Try to sync guid version if the document implements IVersioned and there's a 3rd column
        if (startColumn + 2 < fieldCount && doc is IVersioned versioned)
        {
            try
            {
                var val = reader.GetValue(startColumn + 2);
                if (val is Guid guidVer) versioned.Version = guidVer;
            }
            catch { /* column type mismatch, skip */ }
        }
    }
}
