using System.Data.Common;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Polecat.Serialization;

/// <summary>
///     Default serializer implementation using System.Text.Json.
/// </summary>
public class PolecatSerializer : IPolecatSerializer
{
    private readonly JsonSerializerOptions _options;

    public PolecatSerializer() : this(DefaultOptions())
    {
    }

    public PolecatSerializer(JsonSerializerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Provides access to the underlying JsonSerializerOptions for advanced configuration.
    /// </summary>
    public JsonSerializerOptions Options => _options;

    public string ToJson(object document)
    {
        return JsonSerializer.Serialize(document, document.GetType(), _options);
    }

    public T FromJson<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, _options)!;
    }

    public object FromJson(Type type, string json)
    {
        return JsonSerializer.Deserialize(json, type, _options)!;
    }

    public T FromJson<T>(Stream stream)
    {
        return JsonSerializer.Deserialize<T>(stream, _options)!;
    }

    public object FromJson(Type type, Stream stream)
    {
        return JsonSerializer.Deserialize(stream, type, _options)!;
    }

    public T FromJson<T>(DbDataReader reader, int index)
    {
        var json = reader.GetString(index);
        return FromJson<T>(json);
    }

    public object FromJson(Type type, DbDataReader reader, int index)
    {
        var json = reader.GetString(index);
        return FromJson(type, json);
    }

    public async ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
    {
        return (await JsonSerializer.DeserializeAsync<T>(stream, _options, cancellationToken))!;
    }

    public async ValueTask<object> FromJsonAsync(Type type, Stream stream,
        CancellationToken cancellationToken = default)
    {
        return (await JsonSerializer.DeserializeAsync(stream, type, _options, cancellationToken))!;
    }

    public static JsonSerializerOptions DefaultOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false
        };
    }
}
