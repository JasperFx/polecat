using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Polecat.Serialization;

/// <summary>
///     Default serializer implementation using System.Text.Json.
/// </summary>
public class Serializer : ISerializer
{
    private JsonSerializerOptions _options;
    private EnumStorage _enumStorage = EnumStorage.AsInteger;
    private Casing _casing = Casing.CamelCase;
    private CollectionStorage _collectionStorage = CollectionStorage.Default;
    private NonPublicMembersStorage _nonPublicMembersStorage = NonPublicMembersStorage.Default;

    public Serializer() : this(DefaultOptions())
    {
    }

    public Serializer(JsonSerializerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Provides access to the underlying JsonSerializerOptions for advanced configuration.
    /// </summary>
    public JsonSerializerOptions Options => _options;

    public EnumStorage EnumStorage
    {
        get => _enumStorage;
        set
        {
            _enumStorage = value;
            ApplyEnumStorage();
        }
    }

    public Casing Casing
    {
        get => _casing;
        set
        {
            _casing = value;
            ApplyCasing();
        }
    }

    public CollectionStorage CollectionStorage
    {
        get => _collectionStorage;
        set => _collectionStorage = value;
    }

    public NonPublicMembersStorage NonPublicMembersStorage
    {
        get => _nonPublicMembersStorage;
        set
        {
            _nonPublicMembersStorage = value;
            ApplyNonPublicMembers();
        }
    }

    /// <summary>
    ///     Apply custom configuration to the underlying JsonSerializerOptions.
    /// </summary>
    public void Configure(Action<JsonSerializerOptions> configure)
    {
        configure(_options);
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Serialize uses reflection over document's runtime type.")]
    [RequiresDynamicCode("STJ JsonSerializer.Serialize requires runtime code generation for non-source-generated types.")]
    public string ToJson(object document)
    {
        return JsonSerializer.Serialize(document, document.GetType(), _options);
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize<T> uses reflection over T.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public T FromJson<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, _options)!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize uses reflection over the supplied type.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public object FromJson(Type type, string json)
    {
        return JsonSerializer.Deserialize(json, type, _options)!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize<T> uses reflection over T.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public T FromJson<T>(Stream stream)
    {
        return JsonSerializer.Deserialize<T>(stream, _options)!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize uses reflection over the supplied type.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public object FromJson(Type type, Stream stream)
    {
        return JsonSerializer.Deserialize(stream, type, _options)!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize<T> uses reflection over T.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public T FromJson<T>(DbDataReader reader, int index)
    {
        var json = reader.GetString(index);
        return FromJson<T>(json);
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize uses reflection over the supplied type.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public object FromJson(Type type, DbDataReader reader, int index)
    {
        var json = reader.GetString(index);
        return FromJson(type, json);
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.DeserializeAsync<T> uses reflection over T.")]
    [RequiresDynamicCode("STJ JsonSerializer.DeserializeAsync requires runtime code generation for non-source-generated types.")]
    public async ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
    {
        return (await JsonSerializer.DeserializeAsync<T>(stream, _options, cancellationToken))!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.DeserializeAsync uses reflection over the supplied type.")]
    [RequiresDynamicCode("STJ JsonSerializer.DeserializeAsync requires runtime code generation for non-source-generated types.")]
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

    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "JsonStringEnumConverter(JsonNamingPolicy, bool) requires runtime code generation for non-source-generated enum types. The default Serializer is reflection-based by design; AOT consumers should supply a custom ISerializer with the generic JsonStringEnumConverter<TEnum> per-enum.")]
    private void ApplyEnumStorage()
    {
        // Remove any existing JsonStringEnumConverter
        for (var i = _options.Converters.Count - 1; i >= 0; i--)
        {
            if (_options.Converters[i] is JsonStringEnumConverter)
            {
                _options.Converters.RemoveAt(i);
            }
        }

        if (_enumStorage == EnumStorage.AsString)
        {
            _options.Converters.Add(new JsonStringEnumConverter(_options.PropertyNamingPolicy));
        }
    }

    private void ApplyCasing()
    {
        _options.PropertyNamingPolicy = _casing switch
        {
            Casing.CamelCase => JsonNamingPolicy.CamelCase,
            Casing.SnakeCase => JsonNamingPolicy.SnakeCaseLower,
            Casing.Default => null,
            _ => null
        };
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "DefaultJsonTypeInfoResolver uses reflection. The whole NonPublicMembers feature is reflection-driven by intent; AOT consumers should supply a custom ISerializer with an STJ source-generator context.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Same as IL2026.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075:DynamicallyAccessedMembers",
        Justification = "Falls back to scanning typeInfo.Type.GetProperties when the JSON property has no usable AttributeProvider. Reflection on user types — keeping public/non-public properties alive is the user's responsibility when opting into NonPublicMembers.")]
    private void ApplyNonPublicMembers()
    {
        if (_nonPublicMembersStorage == NonPublicMembersStorage.Default)
        {
            return;
        }

        var resolver = new DefaultJsonTypeInfoResolver();

        if (_nonPublicMembersStorage.HasFlag(NonPublicMembersStorage.NonPublicSetters))
        {
            resolver.Modifiers.Add(typeInfo =>
            {
                if (typeInfo.Kind != JsonTypeInfoKind.Object) return;

                foreach (var property in typeInfo.Properties)
                {
                    if (property.Set == null)
                    {
                        // property.Name is the JSON name (e.g. "name" in camelCase), so look up
                        // by AttributeProvider which gives us the actual PropertyInfo
                        var clrProperty = property.AttributeProvider as System.Reflection.PropertyInfo;
                        if (clrProperty == null)
                        {
                            // Fallback: search all properties by case-insensitive match
                            clrProperty = typeInfo.Type.GetProperties(
                                    System.Reflection.BindingFlags.Public |
                                    System.Reflection.BindingFlags.NonPublic |
                                    System.Reflection.BindingFlags.Instance)
                                .FirstOrDefault(p => string.Equals(p.Name, property.Name,
                                    StringComparison.OrdinalIgnoreCase));
                        }

                        if (clrProperty?.GetSetMethod(true) != null)
                        {
                            property.Set = (obj, value) => clrProperty.GetSetMethod(true)!.Invoke(obj, [value]);
                        }
                    }
                }
            });
        }

        _options.TypeInfoResolver = resolver;

        if (_nonPublicMembersStorage.HasFlag(NonPublicMembersStorage.NonPublicDefaultConstructor) ||
            _nonPublicMembersStorage.HasFlag(NonPublicMembersStorage.NonPublicConstructor))
        {
            _options.PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate;
        }
    }
}
