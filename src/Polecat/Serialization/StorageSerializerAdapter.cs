using System.Buffers;
using System.Data.Common;
using System.Text;
using Weasel.Storage;

namespace Polecat.Serialization;

/// <summary>
///     Adapts any Polecat <see cref="ISerializer" /> to the dialect-neutral
///     <see cref="IStorageSerializer" /> seam of the shared closed-shape storage runtime (#273).
///     The default <see cref="Serializer" /> implements the seam natively and never needs this;
///     the adapter covers user-supplied serializers, deriving the seam-only members
///     (<c>ToCleanJson</c>, <c>WriteTo</c>, <c>WriteToParameter</c>, async reader reads) from the
///     shared <c>Weasel.Core.ISerializer</c> members every Polecat serializer already carries.
/// </summary>
internal sealed class StorageSerializerAdapter : IStorageSerializer
{
    private readonly ISerializer _inner;

    private StorageSerializerAdapter(ISerializer inner)
    {
        _inner = inner;
    }

    /// <summary>
    ///     Returns the serializer itself when it already implements the seam (the default
    ///     <see cref="Serializer" /> does), otherwise wraps it.
    /// </summary>
    public static IStorageSerializer For(ISerializer serializer)
    {
        return serializer as IStorageSerializer ?? new StorageSerializerAdapter(serializer);
    }

    public string ToJson(object? document)
    {
        return document is null ? "null" : _inner.ToJson(document);
    }

    /// <summary>
    ///     STJ-only Polecat has no type-metadata pollution to strip; "clean" JSON is plain JSON.
    /// </summary>
    public string ToCleanJson(object? document) => ToJson(document);

    public void WriteTo(IBufferWriter<byte> writer, object? value)
    {
        var json = ToJson(value);
        writer.Write(Encoding.UTF8.GetBytes(json));
    }

    /// <summary>
    ///     Polecat binds JSON as string parameter values throughout (SQL Server 2025's native JSON
    ///     column type accepts nvarchar input), so this stays dialect-neutral.
    /// </summary>
    public void WriteToParameter(DbParameter parameter, object? value)
    {
        parameter.Value = value is null ? DBNull.Value : ToJson(value);
    }

    public T FromJson<T>(Stream stream) => _inner.FromJson<T>(stream);

    public object FromJson(Type type, Stream stream) => _inner.FromJson(type, stream);

    public T FromJson<T>(DbDataReader reader, int index) => _inner.FromJson<T>(reader, index);

    public object FromJson(Type type, DbDataReader reader, int index) => _inner.FromJson(type, reader, index);

    public ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
        => _inner.FromJsonAsync<T>(stream, cancellationToken);

    public ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default)
        => _inner.FromJsonAsync(type, stream, cancellationToken);

    public async ValueTask<T> FromJsonAsync<T>(DbDataReader reader, int index,
        CancellationToken cancellationToken = default)
    {
        // Route through the unannotated Stream overload rather than the RUC-annotated
        // string overloads so the adapter adds no trim/AOT surface of its own.
        await Task.CompletedTask; // row is already buffered; stream access is synchronous
        await using var stream = reader.GetStream(index);
        return _inner.FromJson<T>(stream);
    }

    public async ValueTask<object> FromJsonAsync(Type type, DbDataReader reader, int index,
        CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        await using var stream = reader.GetStream(index);
        return _inner.FromJson(type, stream);
    }
}
