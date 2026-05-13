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
        var doc = _serializer.FromJson(_type, json);

        if (doc == null) return null;

        // Sync metadata if available and extra columns are present
        SyncMetadata(doc, reader, startColumn);

        return doc;
    }

    public override Task<object?> ReadValueAsync(DbDataReader reader, int startColumn, CancellationToken token)
    {
        return Task.FromResult(ReadValue(reader, startColumn));
    }

    private void SyncMetadata(object doc, DbDataReader reader, int startColumn)
    {
        var fieldCount = reader.FieldCount;

        // Try to sync version if the document implements IRevisioned and there's a 3rd column
        if (startColumn + 2 < fieldCount && doc is IRevisioned revisioned)
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
