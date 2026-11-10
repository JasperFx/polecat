using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Polecat.Serialization;

/// <summary>
///     Serialization abstraction for Polecat. Uses System.Text.Json exclusively. Extends the shared
///     <see cref="Weasel.Core.ISerializer" /> (the intersection both Critter Stack document stores rely
///     on) with Polecat's UTF-8 span and buffer-writer overloads (#801). The string-based
///     <c>FromJson</c> overloads it used to re-declare are on that base since weasel#742 (#807).
/// </summary>
/// <remarks>
///     The default <see cref="Serializer" /> implementation routes through
///     <c>System.Text.Json.JsonSerializer</c>, which the .NET trimmer cannot statically analyse.
///     AOT-publishing apps should plug in a custom <c>ISerializer</c> implementation backed by an STJ
///     source-generator context (<c>JsonSerializerContext</c>) rather than the reflection-based default.
///     The shared base is intentionally unannotated (for Marten parity); the concrete
///     <see cref="Serializer" /> carries its own trim/AOT suppressions.
/// </remarks>
public interface ISerializer : Weasel.Core.ISerializer
{
    /// <summary>
    ///     Deserialize UTF-8 encoded JSON bytes to an object of type T without materialising a string.
    ///     The default implementation decodes to a string and calls <see cref="FromJson{T}(string)" />,
    ///     so existing custom serializers keep working; <see cref="Serializer" /> overrides it to hand the
    ///     bytes straight to System.Text.Json.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    T FromJson<T>(ReadOnlySpan<byte> utf8Json) => FromJson<T>(System.Text.Encoding.UTF8.GetString(utf8Json));

    /// <summary>
    ///     Deserialize UTF-8 encoded JSON bytes to an object of the specified type. See
    ///     <see cref="FromJson{T}(ReadOnlySpan{byte})" />.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    object FromJson(Type type, ReadOnlySpan<byte> utf8Json) => FromJson(type, System.Text.Encoding.UTF8.GetString(utf8Json));

    /// <summary>
    ///     Serialize a value as UTF-8 JSON straight into <paramref name="writer" /> (a response
    ///     <c>PipeWriter</c>, a pooled buffer) with no intermediate string. The default implementation
    ///     goes through <see cref="Weasel.Core.ISerializer.ToJson" />.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    void ToJson(object document, System.Buffers.IBufferWriter<byte> writer)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(ToJson(document));
        writer.Write((ReadOnlySpan<byte>)bytes);
    }
}
