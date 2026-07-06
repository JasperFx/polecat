using Polecat.Serialization;

namespace Polecat.Tests.Serialization;

public class SerializerTests
{
    private readonly Serializer _serializer = new();

    [Fact]
    public void can_round_trip_a_simple_object()
    {
        var original = new TestDocument { Id = Guid.NewGuid(), Name = "Test", Count = 42 };

        var json = _serializer.ToJson(original);
        var deserialized = _serializer.FromJson<TestDocument>(json);

        deserialized.Id.ShouldBe(original.Id);
        deserialized.Name.ShouldBe(original.Name);
        deserialized.Count.ShouldBe(original.Count);
    }

    [Fact]
    public void serializes_with_camel_case_by_default()
    {
        var doc = new TestDocument { Id = Guid.NewGuid(), Name = "Test", Count = 1 };

        var json = _serializer.ToJson(doc);

        json.ShouldContain("\"name\":");
        json.ShouldContain("\"count\":");
        json.ShouldNotContain("\"Name\":", Case.Sensitive);
    }

    [Fact]
    public void can_round_trip_with_untyped_deserialization()
    {
        var original = new TestDocument { Id = Guid.NewGuid(), Name = "Untyped", Count = 7 };

        var json = _serializer.ToJson(original);
        var deserialized = (TestDocument)_serializer.FromJson(typeof(TestDocument), json);

        deserialized.Id.ShouldBe(original.Id);
        deserialized.Name.ShouldBe(original.Name);
    }

    [Fact]
    public void can_round_trip_via_stream()
    {
        var original = new TestDocument { Id = Guid.NewGuid(), Name = "Stream", Count = 99 };
        var json = _serializer.ToJson(original);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        var deserialized = _serializer.FromJson<TestDocument>(stream);

        deserialized.Id.ShouldBe(original.Id);
        deserialized.Name.ShouldBe(original.Name);
    }

    [Fact]
    public async Task can_round_trip_via_stream_async()
    {
        var original = new TestDocument { Id = Guid.NewGuid(), Name = "Async", Count = 55 };
        var json = _serializer.ToJson(original);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        var deserialized = await _serializer.FromJsonAsync<TestDocument>(stream);

        deserialized.Id.ShouldBe(original.Id);
        deserialized.Name.ShouldBe(original.Name);
    }

    [Fact]
    public void ignores_null_values_by_default()
    {
        var doc = new TestDocument { Id = Guid.NewGuid(), Name = null!, Count = 1 };

        var json = _serializer.ToJson(doc);

        json.ShouldNotContain("\"name\":");
    }

    // ---- Weasel.Storage.IStorageSerializer seam (#273) ----

    [Fact]
    public void is_the_shared_storage_serializer_seam()
    {
        _serializer.ShouldBeAssignableTo<Weasel.Storage.IStorageSerializer>();
    }

    [Fact]
    public void to_json_of_null_yields_json_null()
    {
        _serializer.ToJson(null).ShouldBe("null");
    }

    [Fact]
    public void to_clean_json_matches_to_json_for_stj()
    {
        var doc = new TestDocument { Id = Guid.NewGuid(), Name = "Clean", Count = 3 };

        _serializer.ToCleanJson(doc).ShouldBe(_serializer.ToJson(doc));
    }

    [Fact]
    public void write_to_buffer_writer_produces_the_same_json()
    {
        var doc = new TestDocument { Id = Guid.NewGuid(), Name = "Buffered", Count = 9 };

        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        _serializer.WriteTo(buffer, doc);

        System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan).ShouldBe(_serializer.ToJson(doc));
    }

    [Fact]
    public void write_to_parameter_assigns_json_string_value()
    {
        var doc = new TestDocument { Id = Guid.NewGuid(), Name = "Param", Count = 4 };
        var parameter = new Microsoft.Data.SqlClient.SqlParameter();

        _serializer.WriteToParameter(parameter, doc);

        parameter.Value.ShouldBe(_serializer.ToJson(doc));
    }

    [Fact]
    public void write_to_parameter_assigns_dbnull_for_null()
    {
        var parameter = new Microsoft.Data.SqlClient.SqlParameter();

        _serializer.WriteToParameter(parameter, null);

        parameter.Value.ShouldBe(DBNull.Value);
    }

    public class TestDocument
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Count { get; set; }
    }
}
