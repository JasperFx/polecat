using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;

namespace Polecat.AspNetCore;

/// <summary>
/// Minimal-API endpoint return value that writes the high level metadata of a single event stream —
/// Polecat's <c>StreamState</c> — to the <see cref="HttpContext"/> response as JSON. Backed by
/// <see cref="FetchStreamStatePlan"/>, the same query plan that can be batched through
/// <c>IBatchedQuery.QueryByPlan()</c>.
/// <para>
/// Returns HTTP <c>404</c> when the stream does not exist, <see cref="OnFoundStatus"/> (default 200)
/// when it does.
/// </para>
/// <para>
/// The response body is a <see cref="StreamStateResponse"/>, not Polecat's <c>StreamState</c>
/// directly: <c>StreamState.AggregateType</c> is a <see cref="Type"/> and System.Text.Json refuses
/// to serialize those, so the aggregate type is projected down to its simple name.
/// </para>
/// <para>
/// <b>StreamEventState vs StreamAggregate.</b> Use <see cref="StreamEventState"/> when you want the
/// stream's <i>metadata</i> — version, timestamps, archived flag. Use <see cref="StreamAggregate{T}"/>
/// when you want the projected aggregate <i>state</i> built from the stream's events.
/// </para>
/// </summary>
public sealed class StreamEventState : IResult, IEndpointMetadataProvider
{
    private readonly IQuerySession _session;
    private readonly FetchStreamStatePlan _plan;

    /// <summary>
    /// Write the stream metadata for the Guid-identified stream <paramref name="streamId"/>.
    /// </summary>
    public StreamEventState(IQuerySession session, Guid streamId)
        : this(session, new FetchStreamStatePlan(streamId))
    {
    }

    /// <summary>
    /// Write the stream metadata for the string-keyed stream <paramref name="streamKey"/>.
    /// </summary>
    public StreamEventState(IQuerySession session, string streamKey)
        : this(session, new FetchStreamStatePlan(
            streamKey ?? throw new ArgumentNullException(nameof(streamKey))))
    {
    }

    /// <summary>
    /// Write the stream metadata resolved by an existing <see cref="FetchStreamStatePlan"/>. Lets a
    /// handler build the plan once and either batch it or return it straight from an endpoint.
    /// </summary>
    public StreamEventState(IQuerySession session, FetchStreamStatePlan plan)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
        _plan = plan ?? throw new ArgumentNullException(nameof(plan));
    }

    /// <summary>
    /// Status code written when the stream is found. Defaults to 200.
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
    [RequiresDynamicCode("Serializes StreamStateResponse with System.Text.Json, which uses runtime codegen.")]
    [RequiresUnreferencedCode("Reflects over StreamStateResponse via System.Text.Json.")]
    public Task ExecuteAsync(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        return _session.WriteStreamState(_plan, httpContext, ContentType, OnFoundStatus);
    }

    /// <summary>
    /// Populates endpoint metadata so OpenAPI correctly advertises a
    /// <c>200: StreamStateResponse</c> and <c>404</c> response for this endpoint.
    /// </summary>
    public static void PopulateMetadata(MethodInfo method, EndpointBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status200OK, typeof(StreamStateResponse), ["application/json"]));
        builder.Metadata.Add(new ProducesResponseTypeMetadata(
            StatusCodes.Status404NotFound, typeof(void), []));
    }
}
