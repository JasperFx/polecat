using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
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

    /// <summary>
    /// When <c>true</c> (the default), an <c>ETag</c> response header carrying the document's
    /// version is emitted on a hit, and an incoming <c>If-None-Match</c> that matches the current
    /// version yields <c>304 Not Modified</c> with an empty body. Set to <c>false</c> to restore
    /// the pre-ETag behavior exactly (no header, no conditional handling).
    /// </summary>
    public bool EmitETag { get; init; } = true;

    /// <inheritdoc />
    [UnconditionalSuppressMessage("Trimming", "IL2046",
        Justification = "IResult.ExecuteAsync is not RUC-annotated; the contract lives on this override.")]
    [UnconditionalSuppressMessage("AOT", "IL3051",
        Justification = "IResult.ExecuteAsync is not RDC-annotated; the contract lives on this override.")]
    [RequiresDynamicCode("StreamOne<T>.ExecuteAsync routes through the Polecat LINQ provider, which closes generic handler types over T via MakeGenericType.")]
    [RequiresUnreferencedCode("StreamOne<T>.ExecuteAsync routes through the Polecat LINQ provider, which reflects over T. AOT consumers must preserve T's members through DynamicallyAccessedMembers or source generation.")]
    public async Task ExecuteAsync(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        // Read the document JSON and its version together in one round trip so a 304 needs no
        // extra query and a 200 streams the raw persisted JSON with no reserialize.
        var result = await _queryable.ToJsonFirstWithVersionAsync(httpContext.RequestAborted);

        if (result is null)
        {
            httpContext.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        if (EmitETag)
        {
            var etag = ETagHelpers.Format(result.Version);

            if (ETagHelpers.IfNoneMatchMatches(httpContext, etag))
            {
                httpContext.Response.StatusCode = StatusCodes.Status304NotModified;
                httpContext.Response.Headers.ETag = etag;
                httpContext.Response.ContentLength = 0;
                return;
            }

            httpContext.Response.Headers.ETag = etag;
        }

        httpContext.Response.StatusCode = OnFoundStatus;
        httpContext.Response.ContentType = ContentType;
        var json = Encoding.UTF8.GetBytes(result.Json);
        httpContext.Response.ContentLength = json.Length;
        await httpContext.Response.Body.WriteAsync(json, httpContext.RequestAborted);
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
            StatusCodes.Status304NotModified, typeof(void), []));
        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status404NotFound, typeof(void), []));
    }
}
