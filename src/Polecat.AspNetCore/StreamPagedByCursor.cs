using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Polecat.Linq.CursorPaging;

namespace Polecat.AspNetCore;

/// <summary>
/// Minimal-API endpoint return value that streams one keyset (cursor / seek) page of Polecat
/// documents as a JSON envelope directly to the HTTP response, and echoes the continuation cursor
/// in a <c>Polecat-Continuation</c> response header.
/// <para>
/// The body shape mirrors Marten's <c>StreamPagedByCursor</c>:
/// <code>{"items":[...],"nextCursor":"v1:..."}</code>
/// where <c>nextCursor</c> is <c>null</c> at the end of the set. Keyset pagination is constant
/// cost at any depth — ideal for infinite scroll / "load more" / export feeds.
/// </para>
/// <para>
/// The wrapped query must order so its terminal key is the document identity (e.g.
/// <c>OrderBy(x =&gt; x.Foo).ThenBy(x =&gt; x.Id)</c>); a query lacking an OrderBy or with a
/// non-identity terminal key is rejected.
/// </para>
/// </summary>
/// <typeparam name="T">The document type contained in the page.</typeparam>
public sealed class StreamPagedByCursor<T> : IResult, IEndpointMetadataProvider
{
    private readonly IQueryable<T> _queryable;
    private readonly string? _cursor;
    private readonly int _pageSize;

    /// <summary>
    /// Create a <see cref="StreamPagedByCursor{T}"/> over a Polecat <see cref="IQueryable{T}"/>.
    /// </summary>
    /// <param name="queryable">The ordered query (terminal key must be the document identity).</param>
    /// <param name="cursor">The opaque continuation cursor, or <c>null</c> for the first page.</param>
    /// <param name="pageSize">Page size. Must be &gt;= 1.</param>
    public StreamPagedByCursor(IQueryable<T> queryable, string? cursor, int pageSize)
    {
        _queryable = queryable ?? throw new ArgumentNullException(nameof(queryable));
        _cursor = cursor;
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
    [RequiresDynamicCode("StreamPagedByCursor<T>.ExecuteAsync routes through the Polecat LINQ provider, which closes generic handler types over T via MakeGenericType.")]
    [RequiresUnreferencedCode("StreamPagedByCursor<T>.ExecuteAsync routes through the Polecat LINQ provider, which reflects over T. AOT consumers must preserve T's members through DynamicallyAccessedMembers or source generation.")]
    public async Task ExecuteAsync(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var page = await _queryable.ToJsonPageByCursorAsync(_cursor, _pageSize, httpContext.RequestAborted);

        httpContext.Response.StatusCode = OnFoundStatus;
        httpContext.Response.ContentType = ContentType;

        if (page.NextCursor != null)
        {
            httpContext.Response.Headers[CursorPagination.ContinuationHeader] = page.NextCursor;
        }

        var nextCursorJson = page.NextCursor is null
            ? "null"
            : JsonEncodedString(page.NextCursor);

        var envelope = $"{{\"items\":{page.ItemsJson},\"nextCursor\":{nextCursorJson}}}";
        await httpContext.Response.Body.WriteAsync(Encoding.UTF8.GetBytes(envelope), httpContext.RequestAborted);
    }

    // The cursor is "v1:" + base64 (chars A-Za-z0-9+/=:), none of which require JSON escaping.
    // Escape the JSON metacharacters defensively anyway and wrap in quotes — AOT-clean, no STJ.
    private static string JsonEncodedString(string value)
    {
        var escaped = value.Replace("\\", "\\\\").Replace("\"", "\\\"");
        return $"\"{escaped}\"";
    }

    /// <summary>
    /// Populates endpoint metadata so OpenAPI advertises a <c>200</c> JSON response. No 404 is
    /// advertised because an empty page is a valid response.
    /// </summary>
    public static void PopulateMetadata(MethodInfo method, EndpointBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status200OK, typeof(object), ["application/json"]));
    }
}
