using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace Polecat.AspNetCore;

/// <summary>
/// #370: the body writers behind <see cref="StreamEventState"/> and <see cref="StreamEvents"/>,
/// exposed as <see cref="IQuerySession"/> extensions so a handler that does not want the
/// <c>IResult</c> wrapper can write the same responses itself.
/// </summary>
public static class EventStreamExtensions
{
    /// <summary>
    /// Resolve a <see cref="FetchStreamStatePlan"/> and write the resulting stream metadata to the
    /// <paramref name="context"/> response as JSON, or <c>404</c> when the stream does not exist.
    /// <para>
    /// The response body is a <see cref="StreamStateResponse"/> rather than Polecat's
    /// <c>StreamState</c> — see that type for why.
    /// </para>
    /// </summary>
    /// <param name="session"></param>
    /// <param name="plan"></param>
    /// <param name="context"></param>
    /// <param name="contentType"></param>
    /// <param name="onFoundStatus">Defaults to 200</param>
    [RequiresDynamicCode("Serializes StreamStateResponse with System.Text.Json, which uses runtime codegen.")]
    [RequiresUnreferencedCode("Reflects over StreamStateResponse via System.Text.Json.")]
    public static async Task WriteStreamState(
        this IQuerySession session,
        FetchStreamStatePlan plan,
        HttpContext context,
        string contentType = "application/json",
        int onFoundStatus = StatusCodes.Status200OK)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(context);

        var state = await plan.Fetch(session, context.RequestAborted).ConfigureAwait(false);

        if (state == null)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            context.Response.ContentLength = 0;
            return;
        }

        await WriteJson(StreamStateResponse.From(state), context, contentType, onFoundStatus)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Resolve a <see cref="FetchStreamPlan"/> and write the resulting raw events to the
    /// <paramref name="context"/> response as a JSON array.
    /// <para>
    /// <c>FetchStream</c> yields an empty list both for a stream that does not exist and for a filter
    /// that excludes every event, so the two cases cannot be told apart here.
    /// <paramref name="onEmptyStatus"/> decides which answer the endpoint gives; it defaults to
    /// <c>404</c> to match the other single-resource results. Pass <c>200</c> to return an empty JSON
    /// array instead.
    /// </para>
    /// <para>
    /// Each element is an <see cref="EventResponse"/> rather than Polecat's <c>IEvent</c> — see that
    /// type for why.
    /// </para>
    /// </summary>
    /// <param name="session"></param>
    /// <param name="plan"></param>
    /// <param name="context"></param>
    /// <param name="contentType"></param>
    /// <param name="onFoundStatus">Defaults to 200</param>
    /// <param name="onEmptyStatus">Defaults to 404</param>
    [RequiresDynamicCode("Serializes EventResponse[] with System.Text.Json, which uses runtime codegen for each event's Data payload.")]
    [RequiresUnreferencedCode("Reflects over EventResponse and each event's Data payload via System.Text.Json.")]
    public static async Task WriteEvents(
        this IQuerySession session,
        FetchStreamPlan plan,
        HttpContext context,
        string contentType = "application/json",
        int onFoundStatus = StatusCodes.Status200OK,
        int onEmptyStatus = StatusCodes.Status404NotFound)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(context);

        var events = await plan.Fetch(session, context.RequestAborted).ConfigureAwait(false);

        if (events.Count == 0 && onEmptyStatus == StatusCodes.Status404NotFound)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            context.Response.ContentLength = 0;
            return;
        }

        await WriteJson(EventResponse.From(events), context, contentType,
            events.Count == 0 ? onEmptyStatus : onFoundStatus).ConfigureAwait(false);
    }

    /// <summary>
    /// Serialize <paramref name="value"/> and write it to the response with <c>Content-Length</c> set.
    /// Buffers through an <see cref="ArrayBufferWriter{T}"/> so the JSON never round-trips through a
    /// .NET string on its way to the socket.
    /// </summary>
    [RequiresDynamicCode("Serializes the response DTO with System.Text.Json, which uses runtime codegen for the event Data payload.")]
    [RequiresUnreferencedCode("Reflects over the response DTO and the event Data payload via System.Text.Json.")]
    private static async Task WriteJson<T>(T value, HttpContext context, string contentType, int statusCode)
    {
        var buffer = new ArrayBufferWriter<byte>();
        await using (var writer = new Utf8JsonWriter(buffer))
        {
            JsonSerializer.Serialize(writer, value);
        }

        context.Response.StatusCode = statusCode;
        context.Response.ContentType = contentType;
        context.Response.ContentLength = buffer.WrittenCount;
        await context.Response.Body.WriteAsync(buffer.WrittenMemory, context.RequestAborted)
            .ConfigureAwait(false);
    }
}
