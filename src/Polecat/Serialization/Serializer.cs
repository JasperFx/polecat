using System.Buffers;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Weasel.Core;

namespace Polecat.Serialization;

/// <summary>
///     Default serializer implementation using System.Text.Json.
/// </summary>
/// <remarks>
///     Everything but what is Polecat's own comes from Weasel.Core's
///     <see cref="SystemTextJsonSerializer" /> (weasel#555), the copy Polecat and Fisher used to carry
///     byte for byte (#807). What stays here: the native <c>json</c> column's number rules
///     (<see cref="WritesToNativeJson" />, #879) and the UTF-8 span and buffer-writer overloads of
///     Polecat's <see cref="ISerializer" /> (#801).
///     <para>
///         Also implements <see cref="Weasel.Storage.IStorageSerializer" />, the dialect-neutral
///         serializer seam of the shared closed-shape storage runtime (weasel#329/#331, #273), and
///         every member of it but the two re-implemented below is inherited.
///     </para>
/// </remarks>
public class Serializer : SystemTextJsonSerializer, ISerializer, Weasel.Storage.IStorageSerializer
{
    public Serializer()
    {
    }

    public Serializer(JsonSerializerOptions options) : base(options)
    {
    }

    /// <summary>
    ///     Whether what this writes to a parameter ends up in a native <c>json</c> column, which holds a
    ///     number differently from how it is written (#879). Set by <see cref="DocumentStore" /> from
    ///     <see cref="StoreOptions.UseNativeJsonType" />, so a single instance serves one kind of storage.
    /// </summary>
    /// <remarks>
    ///     This is here rather than on a wrapper because the storage runtime is handed this very
    ///     object. <see cref="SystemTextJsonSerializer.ToJson(object?)" /> is deliberately left out of
    ///     it: it also writes things that are never stored, such as a live aggregate, and a value
    ///     doesn't have to fit in a column for that.
    /// </remarks>
    internal bool WritesToNativeJson { get; set; }

    private const string SharedSerializerSuppression =
        "Reflection-based STJ serialize/deserialize is Polecat's documented default; the shared Weasel.Core.ISerializer base is intentionally unannotated for Marten parity, so RUC/RDC is not propagated. AOT consumers supply a source-generator-backed ISerializer.";

    // The base's WriteTo and WriteToParameter are not virtual, so these hide them. The storage
    // runtime only reaches them through Weasel.Storage.IStorageSerializer, which this class
    // re-declares, so the interface maps onto these rather than the base's.

    /// <inheritdoc cref="SystemTextJsonSerializer.WriteTo" />
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public new void WriteTo(IBufferWriter<byte> writer, object? value)
    {
        if (WritesToNativeJson)
        {
            writer.Write(Encoding.UTF8.GetBytes(NativeJsonNumbers.ForStorage(ToJson(value))));
            return;
        }

        base.WriteTo(writer, value);
    }

    /// <summary>
    ///     Writes the value's JSON to a database parameter. Polecat binds JSON as string parameter
    ///     values throughout (SQL Server 2025's native JSON column type accepts nvarchar input), so
    ///     this stays dialect-neutral with no SqlClient-specific typing.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public new void WriteToParameter(DbParameter parameter, object? value)
    {
        if (value is null)
        {
            parameter.Value = DBNull.Value;
            return;
        }

        var json = ToJson(value);
        parameter.Value = WritesToNativeJson ? NativeJsonNumbers.ForStorage(json) : json;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize<T> uses reflection over T.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public T FromJson<T>(ReadOnlySpan<byte> utf8Json)
    {
        return JsonSerializer.Deserialize<T>(utf8Json, Options)!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize uses reflection over the supplied type.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public object FromJson(Type type, ReadOnlySpan<byte> utf8Json)
    {
        return JsonSerializer.Deserialize(utf8Json, type, Options)!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Serialize uses reflection over the value's type.")]
    [RequiresDynamicCode("STJ JsonSerializer.Serialize requires runtime code generation for non-source-generated types.")]
    public void ToJson(object document, IBufferWriter<byte> writer)
    {
        using var jsonWriter = new Utf8JsonWriter(writer);
        JsonSerializer.Serialize(jsonWriter, document, document.GetType(), Options);
    }
}
