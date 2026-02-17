using System.Data.Common;

namespace Polecat.Serialization;

/// <summary>
///     Serialization abstraction for Polecat. Uses System.Text.Json exclusively.
/// </summary>
public interface IPolecatSerializer
{
    /// <summary>
    ///     Serialize an object to a JSON string.
    /// </summary>
    string ToJson(object document);

    /// <summary>
    ///     Deserialize a JSON string to an object of type T.
    /// </summary>
    T FromJson<T>(string json);

    /// <summary>
    ///     Deserialize a JSON string to an object of the specified type.
    /// </summary>
    object FromJson(Type type, string json);

    /// <summary>
    ///     Deserialize from a Stream.
    /// </summary>
    T FromJson<T>(Stream stream);

    /// <summary>
    ///     Deserialize from a Stream to the specified type.
    /// </summary>
    object FromJson(Type type, Stream stream);

    /// <summary>
    ///     Deserialize from a DbDataReader column.
    /// </summary>
    T FromJson<T>(DbDataReader reader, int index);

    /// <summary>
    ///     Deserialize from a DbDataReader column to the specified type.
    /// </summary>
    object FromJson(Type type, DbDataReader reader, int index);

    /// <summary>
    ///     Async deserialize from a Stream.
    /// </summary>
    ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Async deserialize from a Stream to the specified type.
    /// </summary>
    ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default);
}
