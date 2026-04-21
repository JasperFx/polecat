using System.Reflection;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Polecat.Linq;

namespace Polecat.AspNetCore;

/// <summary>
/// Minimal-API endpoint return value that streams the first matching Polecat document
/// as JSON to the HTTP response.
/// <para>
/// Returns HTTP <c>404</c> if the query produces no result, <see cref="OnFoundStatus"/>
/// (default 200) if it does.
/// </para>
/// <para>
/// <b>StreamOne vs StreamAggregate.</b> Use <see cref="StreamOne{T}"/> for regular
/// documents queried with <c>session.Query&lt;T&gt;()</c>. Use <see cref="StreamAggregate{T}"/>
/// for event-sourced aggregates projected live from the event stream.
/// </para>
/// </summary>
/// <typeparam name="T">The document type to stream.</typeparam>
public sealed class StreamOne<T> : IResult, IEndpointMetadataProvider
{
    private readonly IQueryable<T> _queryable;

    /// <summary>
    /// Create a <see cref="StreamOne{T}"/> wrapping a Polecat <see cref="IQueryable{T}"/>.
    /// The query's first matching document is streamed as JSON; 404 if none.
    /// </summary>
    public StreamOne(IQueryable<T> queryable)
    {
        _queryable = queryable ?? throw new ArgumentNullException(nameof(queryable));
    }

    /// <summary>
    /// Status code written when the query produces a result. Defaults to 200.
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

        var result = await _queryable.FirstOrDefaultAsync();

        if (result is null)
        {
            httpContext.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        httpContext.Response.StatusCode = OnFoundStatus;
        httpContext.Response.ContentType = ContentType;
        var json = JsonSerializer.SerializeToUtf8Bytes(result);
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
