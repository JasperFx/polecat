using System.Buffers;
using System.Data.Common;
using System.Text;
using Weasel.Storage;

namespace Polecat.Serialization;

/// <summary>
///     Holds a serializer that isn't <see cref="Serializer" /> to what a native <c>json</c> column can
///     store (#879). Only the JSON meant for a column changes: <see cref="WriteToParameter" /> and
///     <see cref="WriteTo" />, the same two that <see cref="Serializer.WritesToNativeJson" /> covers.
/// </summary>
internal sealed class NativeJsonStorageSerializer(IStorageSerializer inner): IStorageSerializer
{
    public string ToJson(object? document) => inner.ToJson(document);

    public string ToCleanJson(object? document) => inner.ToCleanJson(document);

    public void WriteTo(IBufferWriter<byte> writer, object? value)
        => writer.Write(Encoding.UTF8.GetBytes(NativeJsonNumbers.ForStorage(inner.ToJson(value))));

    public void WriteToParameter(DbParameter parameter, object? value)
    {
        inner.WriteToParameter(parameter, value);

        if (parameter.Value is string json)
        {
            parameter.Value = NativeJsonNumbers.ForStorage(json);
        }
    }

    public T FromJson<T>(Stream stream) => inner.FromJson<T>(stream);

    public T FromJson<T>(DbDataReader reader, int index) => inner.FromJson<T>(reader, index);

    public object FromJson(Type type, Stream stream) => inner.FromJson(type, stream);

    public object FromJson(Type type, DbDataReader reader, int index) => inner.FromJson(type, reader, index);

    public ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
        => inner.FromJsonAsync<T>(stream, cancellationToken);

    public ValueTask<T> FromJsonAsync<T>(DbDataReader reader, int index, CancellationToken cancellationToken = default)
        => inner.FromJsonAsync<T>(reader, index, cancellationToken);

    public ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default)
        => inner.FromJsonAsync(type, stream, cancellationToken);

    public ValueTask<object> FromJsonAsync(Type type, DbDataReader reader, int index,
        CancellationToken cancellationToken = default)
        => inner.FromJsonAsync(type, reader, index, cancellationToken);
}
