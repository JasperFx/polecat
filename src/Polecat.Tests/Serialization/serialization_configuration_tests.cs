using System.Text.Json;
using System.Text.Json.Serialization;
using Polecat.Serialization;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Serialization;

public class serialization_configuration_tests
{
    [Fact]
    public void default_serializer_uses_camel_case()
    {
        var serializer = new Serializer();

        serializer.Casing.ShouldBe(Casing.CamelCase);

        var json = serializer.ToJson(new CasingTarget { FirstName = "Han", LastName = "Solo" });
        json.ShouldContain("\"firstName\":");
        json.ShouldContain("\"lastName\":");
    }

    [Fact]
    public void configure_enum_as_string()
    {
        var serializer = new Serializer();
        serializer.EnumStorage = EnumStorage.AsString;

        var json = serializer.ToJson(new EnumTarget { Color = Color.Blue });
        json.ShouldContain("\"blue\"");
        json.ShouldNotContain("1");

        var deserialized = serializer.FromJson<EnumTarget>(json);
        deserialized.Color.ShouldBe(Color.Blue);
    }

    [Fact]
    public void configure_enum_as_integer()
    {
        var serializer = new Serializer();
        serializer.EnumStorage = EnumStorage.AsInteger;

        var json = serializer.ToJson(new EnumTarget { Color = Color.Blue });
        json.ShouldContain("1");
        json.ShouldNotContain("\"blue\"");

        var deserialized = serializer.FromJson<EnumTarget>(json);
        deserialized.Color.ShouldBe(Color.Blue);
    }

    [Fact]
    public void configure_casing_default()
    {
        var serializer = new Serializer();
        serializer.Casing = Casing.Default;

        var json = serializer.ToJson(new CasingTarget { FirstName = "Luke", LastName = "Skywalker" });
        json.ShouldContain("\"FirstName\":");
        json.ShouldContain("\"LastName\":");
    }

    [Fact]
    public void configure_casing_snake_case()
    {
        var serializer = new Serializer();
        serializer.Casing = Casing.SnakeCase;

        var json = serializer.ToJson(new CasingTarget { FirstName = "Leia", LastName = "Organa" });
        json.ShouldContain("\"first_name\":");
        json.ShouldContain("\"last_name\":");
    }

    [Fact]
    public void configure_casing_camel_case()
    {
        var serializer = new Serializer();
        serializer.Casing = Casing.CamelCase;

        var json = serializer.ToJson(new CasingTarget { FirstName = "Chewie", LastName = "Wookiee" });
        json.ShouldContain("\"firstName\":");
        json.ShouldContain("\"lastName\":");
    }

    [Fact]
    public void custom_configure_callback()
    {
        var serializer = new Serializer();
        serializer.Configure(opts => opts.WriteIndented = true);

        var json = serializer.ToJson(new CasingTarget { FirstName = "Yoda" });
        json.ShouldContain("\n");
    }

    [Fact]
    public void configure_non_public_setters()
    {
        var serializer = new Serializer();
        serializer.NonPublicMembersStorage = NonPublicMembersStorage.NonPublicSetters;

        var json = """{"name":"Obi-Wan"}""";
        var deserialized = serializer.FromJson<PrivateSetterTarget>(json);
        deserialized.Name.ShouldBe("Obi-Wan");
    }

    [Fact]
    public void can_pass_custom_json_serializer_options()
    {
        var customOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            WriteIndented = true
        };

        var serializer = new Serializer(customOptions);

        var json = serializer.ToJson(new CasingTarget { FirstName = "Mace" });
        json.ShouldContain("\"first_name\":");
        json.ShouldContain("\n");
    }

    [Fact]
    public void store_options_configure_serialization()
    {
        var options = new StoreOptions();
        options.ConfigureSerialization(
            enumStorage: EnumStorage.AsString,
            casing: Casing.SnakeCase);

        var serializer = (Serializer)options.Serializer;
        serializer.EnumStorage.ShouldBe(EnumStorage.AsString);
        serializer.Casing.ShouldBe(Casing.SnakeCase);

        var json = serializer.ToJson(new EnumTarget { Color = Color.Green });
        json.ShouldContain("\"green\"");
        json.ShouldContain("\"color\":");
    }

    [Fact]
    public void store_options_configure_serialization_with_base_options()
    {
        var baseOptions = new JsonSerializerOptions { WriteIndented = true };

        var options = new StoreOptions();
        options.ConfigureSerialization(
            baseOptions,
            enumStorage: EnumStorage.AsString,
            casing: Casing.CamelCase);

        var serializer = (Serializer)options.Serializer;
        var json = serializer.ToJson(new EnumTarget { Color = Color.Red });
        json.ShouldContain("\n");
        json.ShouldContain("\"red\"");
    }

    [Fact]
    public void enum_storage_exposed_on_iserializer()
    {
        ISerializer serializer = new Serializer();
        serializer.EnumStorage.ShouldBe(EnumStorage.AsInteger);
        serializer.Casing.ShouldBe(Casing.CamelCase);
    }

    public class CasingTarget
    {
        public string FirstName { get; set; } = string.Empty;
        public string? LastName { get; set; }
    }

    public class EnumTarget
    {
        public Color Color { get; set; }
    }

    public enum Color
    {
        Red,
        Blue,
        Green
    }

    public class PrivateSetterTarget
    {
        public string Name { get; private set; } = string.Empty;
    }
}

[Collection("integration")]
public class serialization_integration_tests : IntegrationContext
{
    public serialization_integration_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task store_and_load_with_enum_as_string()
    {
        await StoreOptions(opts =>
        {
            opts.ConfigureSerialization(enumStorage: EnumStorage.AsString);
        });

        var id = Guid.NewGuid();
        var doc = new ColorDocument { Id = id, Preference = ColorPreference.Blue };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        var loaded = await theSession.LoadAsync<ColorDocument>(id);
        loaded.ShouldNotBeNull();
        loaded.Preference.ShouldBe(ColorPreference.Blue);
    }

    public class ColorDocument
    {
        public Guid Id { get; set; }
        public ColorPreference Preference { get; set; }
    }

    public enum ColorPreference
    {
        Red,
        Blue,
        Green
    }
}
