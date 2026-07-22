using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Polecat.Linq;

namespace Polecat.AspNetCore;

/// <summary>
/// Minimal-API endpoint return value that streams a single page of Polecat documents plus
/// paging metadata as one JSON envelope directly to the HTTP response, in a single database
/// round trip.
/// <para>
/// The envelope shape is byte-for-byte identical to Marten's <c>StreamPaged</c> so clients
/// are interchangeable:
/// <code>
/// {"pageNumber":3,"pageSize":25,"totalItemCount":1207,"pageCount":49,
///  "hasNextPage":true,"hasPreviousPage":true,"items":[...]}
/// </code>
/// The <c>items</c> array carries raw persisted document JSON — no deserialize/reserialize.
/// </para>
/// <para>
/// Unlike <see cref="StreamOne{T}"/>, this type never returns 404: an empty page yields a
/// well-formed envelope with <c>totalItemCount: 0</c>, <c>pageCount: 0</c> and <c>items: []</c>.
/// </para>
/// </summary>
/// <typeparam name="T">The document type contained in the page.</typeparam>
public sealed class StreamPaged<T> : IResult, IEndpointMetadataProvider
{
    private readonly IQueryable<T> _queryable;
    private readonly int _pageNumber;
    private readonly int _pageSize;

    /// <summary>
    /// Create a <see cref="StreamPaged{T}"/> over a Polecat <see cref="IQueryable{T}"/>.
    /// Include an <c>OrderBy</c> on the queryable for a stable page order.
    /// </summary>
    /// <param name="queryable">The (optionally filtered/ordered) query to page.</param>
    /// <param name="pageNumber">1-based page number. Must be &gt;= 1.</param>
    /// <param name="pageSize">Page size. Must be &gt;= 1.</param>
    public StreamPaged(IQueryable<T> queryable, int pageNumber, int pageSize)
    {
        _queryable = queryable ?? throw new ArgumentNullException(nameof(queryable));
        _pageNumber = pageNumber;
        _pageSize = pageSize;
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
    [UnconditionalSuppressMessage("Trimming", "IL2046",
        Justification = "IResult.ExecuteAsync is not RUC-annotated; the contract lives on this override.")]
    [UnconditionalSuppressMessage("AOT", "IL3051",
        Justification = "IResult.ExecuteAsync is not RDC-annotated; the contract lives on this override.")]
    [RequiresDynamicCode("StreamPaged<T>.ExecuteAsync routes through the Polecat LINQ provider, which closes generic handler types over T via MakeGenericType.")]
    [RequiresUnreferencedCode("StreamPaged<T>.ExecuteAsync routes through the Polecat LINQ provider, which reflects over T. AOT consumers must preserve T's members through DynamicallyAccessedMembers or source generation.")]
    public async Task ExecuteAsync(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        httpContext.Response.StatusCode = OnFoundStatus;
        httpContext.Response.ContentType = ContentType;

        await _queryable.StreamPagedJsonArray(_pageNumber, _pageSize, httpContext.Response.Body,
            httpContext.RequestAborted);
    }

    /// <summary>
    /// Populates endpoint metadata so OpenAPI advertises a <c>200</c> JSON response.
    /// No 404 is advertised because an empty page is a valid response.
    /// </summary>
    public static void PopulateMetadata(MethodInfo method, EndpointBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status200OK, typeof(object), ["application/json"]));
    }
}
