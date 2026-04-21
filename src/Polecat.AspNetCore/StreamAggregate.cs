using System.Reflection;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;

namespace Polecat.AspNetCore;

/// <summary>
/// Minimal-API endpoint return value that streams the latest projected state of an
/// event-sourced aggregate as JSON to the HTTP response.
/// <para>
/// Returns HTTP <c>404</c> if no aggregate exists for the supplied id,
/// <see cref="OnFoundStatus"/> (default 200) if it does.
/// </para>
/// <para>
/// <b>StreamAggregate vs StreamOne.</b> Use <see cref="StreamAggregate{T}"/>
/// for event-sourced aggregates — Polecat rebuilds (or reads the snapshot of) the
/// latest aggregate state from events before streaming. Use <see cref="StreamOne{T}"/>
/// for regular documents that are stored directly (not event-sourced).
/// </para>
/// </summary>
/// <typeparam name="T">The aggregate type.</typeparam>
public sealed class StreamAggregate<T> : IResult, IEndpointMetadataProvider where T : class, new()
{
    private readonly IQuerySession _session;
    private readonly Guid _guidId;
    private readonly string? _stringId;
    private readonly bool _useGuid;

    /// <summary>
    /// Stream the latest aggregate state of type <typeparamref name="T"/> for
    /// the aggregate whose stream is identified by <paramref name="id"/>.
    /// </summary>
    public StreamAggregate(IQuerySession session, Guid id)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
        _guidId = id;
        _stringId = null;
        _useGuid = true;
    }

    /// <summary>
    /// Stream the latest aggregate state of type <typeparamref name="T"/> for
    /// the aggregate whose stream is identified by string <paramref name="id"/>
    /// (for stores configured with string-keyed streams).
    /// </summary>
    public StreamAggregate(IQuerySession session, string id)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
        _stringId = id ?? throw new ArgumentNullException(nameof(id));
        _guidId = Guid.Empty;
        _useGuid = false;
    }

    /// <summary>
    /// Status code written when the aggregate is found. Defaults to 200.
    /// </summary>
    public int OnFoundStatus { get; init; } = StatusCodes.Status200OK;

    /// <summary>
    /// Response content type. Defaults to <c>application/json</c>.
    /// </summary>
    public string ContentType { get; init; } = "application/json";

    /// <inheritdoc />
    public async Task ExecuteAsync(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var aggregate = _useGuid
            ? await _session.Events.FetchLatest<T>(_guidId)
            : await _session.Events.FetchLatest<T>(_stringId!);

        if (aggregate is null)
        {
            httpContext.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        httpContext.Response.StatusCode = OnFoundStatus;
        httpContext.Response.ContentType = ContentType;
        var json = JsonSerializer.SerializeToUtf8Bytes(aggregate);
        httpContext.Response.ContentLength = json.Length;
        await httpContext.Response.Body.WriteAsync(json);
    }

    /// <summary>
    /// Populates endpoint metadata so OpenAPI correctly advertises a
    /// <c>200: T</c> and <c>404</c> response for this endpoint.
    /// </summary>
    public static void PopulateMetadata(MethodInfo method, EndpointBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status200OK, typeof(T), ["application/json"]));
        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status404NotFound, typeof(void), []));
    }
}
