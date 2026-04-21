using System.Reflection;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Polecat.Linq;

namespace Polecat.AspNetCore;

/// <summary>
/// Minimal-API endpoint return value that streams a JSON array of Polecat documents
/// to the HTTP response.
/// <para>
/// Unlike <see cref="StreamOne{T}"/>, this type never returns 404: an empty result
/// set yields an empty JSON array (<c>[]</c>) with status <see cref="OnFoundStatus"/>
/// (default 200).
/// </para>
/// </summary>
/// <typeparam name="T">The document type contained in the array.</typeparam>
public sealed class StreamMany<T> : IResult, IEndpointMetadataProvider
{
    private readonly IQueryable<T> _queryable;

    /// <summary>
    /// Create a <see cref="StreamMany{T}"/> wrapping a Polecat <see cref="IQueryable{T}"/>.
    /// All matching documents are streamed as a JSON array.
    /// </summary>
    public StreamMany(IQueryable<T> queryable)
    {
        _queryable = queryable ?? throw new ArgumentNullException(nameof(queryable));
    }

    /// <summary>
    /// Status code written with the response. Defaults to 200.
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

        var results = await _queryable.ToListAsync();

        httpContext.Response.StatusCode = OnFoundStatus;
        httpContext.Response.ContentType = ContentType;
        var json = JsonSerializer.SerializeToUtf8Bytes(results);
        httpContext.Response.ContentLength = json.Length;
        await httpContext.Response.Body.WriteAsync(json);
    }

    /// <summary>
    /// Populates endpoint metadata so OpenAPI correctly advertises a
    /// <c>200: T[]</c> response for this endpoint. No 404 is advertised
    /// because an empty array is a valid response.
    /// </summary>
    public static void PopulateMetadata(MethodInfo method, EndpointBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status200OK, typeof(IReadOnlyList<T>), ["application/json"]));
    }
}
