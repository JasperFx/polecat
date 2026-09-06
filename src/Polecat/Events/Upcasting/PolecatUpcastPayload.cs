using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using JasperFx.Events.Upcasting;
using Polecat.Serialization;

namespace Polecat.Events.Upcasting;

/// <summary>
///     Polecat's adapter for the shared <see cref="IUpcastPayload" /> seam: one stored
///     <c>pc_events.data</c> value, presented to an upcast transformation without exposing how
///     Polecat read it.
/// </summary>
/// <remarks>
///     <para>
///         #561 / jasperfx#752. The shared contract abstracts Marten's
///         <c>(ISerializer, DbDataReader, index)</c> triple, which is PostgreSQL- and
///         Marten-serializer-shaped, down to "a payload that can hand you either a deserialized old
///         CLR type or the raw JSON". Polecat's half of that is unusually small, because by the time
///         a row reaches here the JSON has already been pulled off the reader as a string: this type
///         is a two-field struct-shaped adapter over <c>(json, serializer)</c> and nothing else.
///     </para>
///     <para>
///         <b>The async members are deliberately synchronous underneath.</b> The contract offers them
///         so a store whose read path can stream a payload rather than buffer it has somewhere to do
///         that; Polecat's cannot and does not need to — the string is already materialized. What the
///         async members are actually load-bearing for is the OTHER half of the split: an async-only
///         transformation must be reachable, and it is reached through
///         <see cref="IUpcastPayload.AsAsync{T}" />. Wrapping a completed value in a
///         <see cref="ValueTask{TResult}" /> allocates nothing.
///     </para>
///     <para>
///         <see cref="AsJsonDocument" /> never throws the way Marten's can. The contract allows a
///         store whose serializer cannot produce a <see cref="JsonDocument" /> to refuse; Polecat is
///         System.Text.Json only and stores event bodies in a SQL Server 2025 native <c>json</c>
///         column, so the raw-JSON form is always available. That is what makes
///         <c>JasperFx.Events.Upcasting.SystemTextJson</c>'s upcasters — the ones that let the old
///         CLR type be deleted from the codebase entirely — unconditionally usable here.
///     </para>
///     <para>
///         Short-lived and single-use by design: the contract says a transformation calls exactly one
///         accessor exactly once per event, so nothing here is cached and the parsed document is the
///         caller's to dispose.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: As<T>() routes through ISerializer.FromJson. The old event type is named in the consumer's upcast registration, so it is rooted by that call site; AOT consumers supply a source-generator-backed ISerializer impl.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson is annotated RDC.")]
internal sealed class PolecatUpcastPayload : IUpcastPayload
{
    private readonly string _json;
    private readonly ISerializer _serializer;

    public PolecatUpcastPayload(string json, ISerializer serializer)
    {
        _json = json;
        _serializer = serializer;
    }

    public T As<T>() where T : notnull => _serializer.FromJson<T>(_json);

    public ValueTask<T> AsAsync<T>(CancellationToken token) where T : notnull
        => new(_serializer.FromJson<T>(_json));

    public JsonDocument AsJsonDocument() => JsonDocument.Parse(_json);

    public ValueTask<JsonDocument> AsJsonDocumentAsync(CancellationToken token)
        => new(JsonDocument.Parse(_json));
}
