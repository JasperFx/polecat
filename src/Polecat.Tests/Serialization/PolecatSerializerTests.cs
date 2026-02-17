using Polecat.Serialization;

namespace Polecat.Tests.Serialization;

public class PolecatSerializerTests
{
    private readonly PolecatSerializer _serializer = new();

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

    public class TestDocument
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Count { get; set; }
    }
}
