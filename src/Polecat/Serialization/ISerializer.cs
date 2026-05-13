using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Polecat.Serialization;

/// <summary>
///     Serialization abstraction for Polecat. Uses System.Text.Json exclusively.
/// </summary>
/// <remarks>
///     The default <see cref="Serializer"/> implementation routes through
///     <c>System.Text.Json.JsonSerializer</c>, which the .NET trimmer cannot
///     statically analyse. AOT-publishing apps should plug in a custom
///     <c>ISerializer</c> implementation backed by an STJ source-generator
///     context (<c>JsonSerializerContext</c>) rather than the reflection-based
///     default.
/// </remarks>
public interface ISerializer
{
    /// <summary>
    ///     The enum storage strategy.
    /// </summary>
    EnumStorage EnumStorage { get; }

    /// <summary>
    ///     The property naming casing strategy.
    /// </summary>
    Casing Casing { get; }

    /// <summary>
    ///     Serialize an object to a JSON string.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection over document.GetType()'s properties; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    string ToJson(object document);

    /// <summary>
    ///     Deserialize a JSON string to an object of type T.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    T FromJson<T>(string json);

    /// <summary>
    ///     Deserialize a JSON string to an object of the specified type.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    object FromJson(Type type, string json);

    /// <summary>
    ///     Deserialize from a Stream.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    T FromJson<T>(Stream stream);

    /// <summary>
    ///     Deserialize from a Stream to the specified type.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    object FromJson(Type type, Stream stream);

    /// <summary>
    ///     Deserialize from a DbDataReader column.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    T FromJson<T>(DbDataReader reader, int index);

    /// <summary>
    ///     Deserialize from a DbDataReader column to the specified type.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    object FromJson(Type type, DbDataReader reader, int index);

    /// <summary>
    ///     Async deserialize from a Stream.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Async deserialize from a Stream to the specified type.
    /// </summary>
    [RequiresUnreferencedCode("Default ISerializer uses STJ reflection; AOT consumers should supply a source-generator-backed ISerializer impl.")]
    [RequiresDynamicCode("Default ISerializer uses STJ reflection which requires runtime code generation.")]
    ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default);
}
