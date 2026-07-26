using System.Text.Json;
using Alba;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

/// <summary>
///     #370 (parity with marten#5053): the <c>StreamEventState</c> / <c>StreamEvents</c> endpoint result
///     types over real Minimal API endpoints.
/// </summary>
public class stream_event_result_types_tests : IAsyncLifetime
{
    private IAlbaHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = await AlbaHost.For<Program>();

        var store = (DocumentStore)_host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    public async Task DisposeAsync() => await _host.DisposeAsync();

    [Fact]
    public async Task stream_event_state_returns_200_with_the_metadata()
    {
        var streamId = await StartPartyAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{streamId}/state");
            s.StatusCodeShouldBeOk();
            s.ContentTypeShouldBe("application/json");
        });

        var state = Read<StreamStateResponse>(result);
        state.Id.ShouldBe(streamId);
        state.Version.ShouldBe(2);
        state.IsArchived.ShouldBeFalse();
        state.Created.ShouldNotBe(default);
        state.LastTimestamp.ShouldNotBe(default);
    }

    [Fact]
    public async Task stream_event_state_returns_404_for_a_missing_stream()
    {
        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{Guid.NewGuid()}/state");
            s.StatusCodeShouldBe(404);
        });
    }

    [Fact]
    public async Task stream_event_state_accepts_a_prebuilt_plan()
    {
        var streamId = await StartPartyAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{streamId}/state-by-plan");
            s.StatusCodeShouldBeOk();
        });

        Read<StreamStateResponse>(result).Version.ShouldBe(2);
    }

    [Fact]
    public async Task stream_events_returns_200_with_the_serialized_events()
    {
        var streamId = await StartPartyAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{streamId}/events");
            s.StatusCodeShouldBeOk();
            s.ContentTypeShouldBe("application/json");
        });

        var events = Read<EventResponse[]>(result);
        events.Length.ShouldBe(2);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
        events.ShouldAllBe(x => x.StreamId == streamId);
        events.ShouldAllBe(x => x.Id != Guid.Empty);
        events.ShouldAllBe(x => x.Sequence > 0);
    }

    /// <summary>
    ///     The event body itself has to survive the DTO projection — an endpoint that returned only
    ///     metadata would pass every other assertion here.
    /// </summary>
    [Fact]
    public async Task stream_events_writes_the_event_body()
    {
        var streamId = await StartPartyAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{streamId}/events");
            s.StatusCodeShouldBeOk();
        });

        var body = result.ReadAsText();
        body.ShouldContain("Fellowship");
        body.ShouldContain("Frodo");
    }

    /// <summary>
    ///     The reason the DTOs exist at all: <c>IEvent.EventType</c> is a <see cref="Type" />, and STJ
    ///     throws NotSupportedException outright on those. <c>EventTypeName</c> is the alias a client
    ///     discriminates on; the assembly-qualified .NET type name is deliberately kept off the wire.
    /// </summary>
    [Fact]
    public async Task stream_events_writes_the_alias_and_not_the_dotnet_type()
    {
        var streamId = await StartPartyAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{streamId}/events");
            s.StatusCodeShouldBeOk();
        });

        var events = Read<EventResponse[]>(result);
        events.ShouldAllBe(x => !string.IsNullOrEmpty(x.EventTypeName));

        var body = result.ReadAsText();
        body.ShouldNotContain("Polecat.AspNetCore.Testing, Version=");
        body.ShouldNotContain("\"EventType\"");
    }

    [Fact]
    public async Task stream_events_returns_404_for_a_missing_stream_by_default()
    {
        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{Guid.NewGuid()}/events");
            s.StatusCodeShouldBe(404);
        });
    }

    [Fact]
    public async Task on_empty_status_opts_out_of_the_404()
    {
        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{Guid.NewGuid()}/events-empty200");
            s.StatusCodeShouldBeOk();
        });

        Read<EventResponse[]>(result).ShouldBeEmpty();
    }

    /// <summary>
    ///     The case OnEmptyStatus exists for: paging forward with fromVersion and running off the end of
    ///     a stream that really does exist is expected, not a 404.
    /// </summary>
    [Fact]
    public async Task on_empty_status_covers_paging_off_the_end_of_a_real_stream()
    {
        var streamId = await StartPartyAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{streamId}/events-empty200?fromVersion=99");
            s.StatusCodeShouldBeOk();
        });

        Read<EventResponse[]>(result).ShouldBeEmpty();
    }

    [Fact]
    public async Task stream_events_accepts_a_prebuilt_plan_and_honors_its_filters()
    {
        var streamId = await StartPartyAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/streams/{streamId}/events-by-plan");
            s.StatusCodeShouldBeOk();
        });

        // The endpoint's plan caps at version 1
        var events = Read<EventResponse[]>(result);
        events.Length.ShouldBe(1);
        events[0].Version.ShouldBe(1);
    }

    [Fact]
    public async Task both_results_set_content_length()
    {
        var streamId = await StartPartyAsync();

        foreach (var url in new[] { $"/api/streams/{streamId}/state", $"/api/streams/{streamId}/events" })
        {
            var result = await _host.Scenario(s =>
            {
                s.Get.Url(url);
                s.StatusCodeShouldBeOk();
            });

            result.Context.Response.ContentLength.ShouldNotBeNull($"{url} must set Content-Length");
            result.Context.Response.ContentLength!.Value.ShouldBeGreaterThan(0);
        }
    }

    [Fact]
    public async Task both_results_advertise_their_openapi_metadata()
    {
        var sources = _host.Services.GetServices<Microsoft.AspNetCore.Routing.EndpointDataSource>();
        var endpoints = sources.SelectMany(x => x.Endpoints).OfType<Microsoft.AspNetCore.Routing.RouteEndpoint>();

        foreach (var pattern in new[] { "/api/streams/{id:guid}/state", "/api/streams/{id:guid}/events" })
        {
            var endpoint = endpoints.Single(x => x.RoutePattern.RawText == pattern);
            var produces = endpoint.Metadata
                .OfType<Microsoft.AspNetCore.Http.Metadata.IProducesResponseTypeMetadata>()
                .ToList();

            produces.ShouldContain(x => x.StatusCode == 200, $"{pattern} should advertise a 200");
            produces.ShouldContain(x => x.StatusCode == 404, $"{pattern} should advertise a 404");
        }
    }

    private async Task<Guid> StartPartyAsync()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();
        var streamId = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new StreamingQuestStarted("Fellowship"),
            new StreamingMembersJoined(["Frodo", "Sam"]));
        await session.SaveChangesAsync();

        return streamId;
    }

    private static T Read<T>(IScenarioResult result)
    {
        return JsonSerializer.Deserialize<T>(result.ReadAsText(),
            new JsonSerializerOptions(JsonSerializerDefaults.Web))!;
    }
}
