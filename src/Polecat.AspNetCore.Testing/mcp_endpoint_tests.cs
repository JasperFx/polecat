using System.Text.Json;
using Alba;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

public class mcp_endpoint_tests : IAsyncLifetime
{
    private IAlbaHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = await AlbaHost.For<Program>();
    }

    public async Task DisposeAsync()
    {
        await _host.DisposeAsync();
    }

    private async Task<JsonDocument> SendMcpRequest(string method, object? @params = null)
    {
        var request = new
        {
            jsonrpc = "2.0",
            id = 1,
            method,
            @params
        };

        var result = await _host.Scenario(s =>
        {
            s.Post.Json(request).ToUrl("/polecat/mcp/");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        var body = result.ReadAsText();
        return JsonDocument.Parse(body);
    }

    [Fact]
    public async Task can_initialize()
    {
        using var doc = await SendMcpRequest("initialize");

        var root = doc.RootElement;
        root.GetProperty("jsonrpc").GetString().ShouldBe("2.0");
        root.GetProperty("id").GetInt32().ShouldBe(1);

        var result = root.GetProperty("result");
        result.GetProperty("protocolVersion").GetString().ShouldNotBeNullOrEmpty();

        var capabilities = result.GetProperty("capabilities");
        capabilities.TryGetProperty("tools", out _).ShouldBeTrue();

        var serverInfo = result.GetProperty("serverInfo");
        serverInfo.GetProperty("name").GetString().ShouldBe("polecat");
    }

    [Fact]
    public async Task can_list_tools()
    {
        using var doc = await SendMcpRequest("tools/list");

        var tools = doc.RootElement.GetProperty("result").GetProperty("tools");
        tools.GetArrayLength().ShouldBeGreaterThanOrEqualTo(3);

        var toolNames = tools.EnumerateArray()
            .Select(t => t.GetProperty("name").GetString())
            .ToList();

        toolNames.ShouldContain("get_event_store_configuration");
        toolNames.ShouldContain("list_known_event_types");
        toolNames.ShouldContain("list_projections");

        foreach (var tool in tools.EnumerateArray())
        {
            tool.GetProperty("description").GetString().ShouldNotBeNullOrEmpty();
            tool.GetProperty("inputSchema").GetProperty("type").GetString().ShouldBe("object");
        }
    }

    [Fact]
    public async Task can_get_event_store_configuration()
    {
        using var doc = await SendMcpRequest("tools/call",
            new { name = "get_event_store_configuration" });

        var content = doc.RootElement.GetProperty("result").GetProperty("content");
        content.GetArrayLength().ShouldBe(1);

        var text = content[0].GetProperty("text").GetString()!;
        using var config = JsonDocument.Parse(text);
        var root = config.RootElement;

        root.GetProperty("streamIdentity").GetString().ShouldBe("AsGuid");
        root.GetProperty("tenancyStyle").GetString().ShouldBe("Single");
        root.GetProperty("databaseSchemaName").GetString().ShouldNotBeNullOrEmpty();
        root.GetProperty("enableCorrelationId").GetBoolean().ShouldBeTrue();
        root.GetProperty("enableCausationId").GetBoolean().ShouldBeTrue();
        root.GetProperty("enableHeaders").GetBoolean().ShouldBeFalse();
    }

    [Fact]
    public async Task can_list_known_event_types()
    {
        using var doc = await SendMcpRequest("tools/call",
            new { name = "list_known_event_types" });

        var content = doc.RootElement.GetProperty("result").GetProperty("content");
        content.GetArrayLength().ShouldBe(1);
        content[0].GetProperty("type").GetString().ShouldBe("text");

        var text = content[0].GetProperty("text").GetString()!;
        using var eventTypes = JsonDocument.Parse(text);
        // The array should be valid JSON (may be empty if no events registered yet)
        eventTypes.RootElement.ValueKind.ShouldBe(JsonValueKind.Array);
    }

    [Fact]
    public async Task can_list_projections()
    {
        using var doc = await SendMcpRequest("tools/call",
            new { name = "list_projections" });

        var content = doc.RootElement.GetProperty("result").GetProperty("content");
        var text = content[0].GetProperty("text").GetString()!;
        using var projections = JsonDocument.Parse(text);
        projections.RootElement.ValueKind.ShouldBe(JsonValueKind.Array);
    }

    [Fact]
    public async Task returns_error_for_unknown_method()
    {
        using var doc = await SendMcpRequest("nonexistent/method");

        var error = doc.RootElement.GetProperty("error");
        error.GetProperty("code").GetInt32().ShouldBe(-32601);
        error.GetProperty("message").GetString()!.ShouldContain("Method not found");
    }

    [Fact]
    public async Task returns_error_for_unknown_tool()
    {
        using var doc = await SendMcpRequest("tools/call",
            new { name = "nonexistent_tool" });

        var error = doc.RootElement.GetProperty("error");
        error.GetProperty("code").GetInt32().ShouldBe(-32602);
        error.GetProperty("message").GetString()!.ShouldContain("Unknown tool");
    }

    [Fact]
    public async Task returns_parse_error_for_invalid_json()
    {
        var result = await _host.Scenario(s =>
        {
            s.Post.Text("this is not json").ToUrl("/polecat/mcp/");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        var body = result.ReadAsText();
        using var doc = JsonDocument.Parse(body);
        var error = doc.RootElement.GetProperty("error");
        error.GetProperty("code").GetInt32().ShouldBe(-32700);
    }
}
