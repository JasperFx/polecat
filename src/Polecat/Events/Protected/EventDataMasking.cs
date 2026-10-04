using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using JasperFx.Events;
using JasperFx.Events.Protected;
using Polecat.Linq;

namespace Polecat.Events.Protected;

/*
 * IEventDataMasking used to be declared here. It was lifted into JasperFx.Events.Protected in
 * jasperfx#635 (2.41.0), beside the StreamCompactingRequest<T> it belongs with: a fluent
 * description of a masking intent is database-agnostic, while executing it is unavoidably
 * store-specific. Marten's copy and this one were member-for-member identical -- ForTenant, four
 * IncludeStream overloads, IncludeEvents, AddHeader, same order and same signatures -- which is
 * what made the lift a straight swap.
 *
 * This DOES move the interface's namespace, so user code that names Polecat.Events.Protected.
 * IEventDataMasking explicitly has to update its using. The common fluent usage,
 * Advanced.ApplyEventDataMasking(x => x.IncludeStream(...)), never names the type.
 */

/// <summary>
///     Implementation of IEventDataMasking that fetches events, applies masking rules,
///     and overwrites the event data in the database.
/// </summary>
public class EventDataMasking : IEventDataMasking
{
    private readonly DocumentStore _store;
    private readonly List<Func<IDocumentSession, CancellationToken, Task<IReadOnlyList<IEvent>>>> _sources = new();
    private readonly Dictionary<string, object> _headers = new();
    private string? _tenantId;

    public EventDataMasking(DocumentStore store)
    {
        _store = store;
    }

    public IEventDataMasking ForTenant(string tenantId)
    {
        _tenantId = tenantId;
        return this;
    }

    public IEventDataMasking IncludeStream(Guid streamId)
    {
        _sources.Add((s, t) => s.Events.FetchStreamAsync(streamId, token: t));
        return this;
    }

    public IEventDataMasking IncludeStream(string streamKey)
    {
        _sources.Add((s, t) => s.Events.FetchStreamAsync(streamKey, token: t));
        return this;
    }

    public IEventDataMasking IncludeStream(Guid streamId, Func<IEvent, bool> filter)
    {
        _sources.Add(async (s, t) =>
        {
            var raw = await s.Events.FetchStreamAsync(streamId, token: t).ConfigureAwait(false);
            return raw.Where(filter).ToList();
        });
        return this;
    }

    public IEventDataMasking IncludeStream(string streamKey, Func<IEvent, bool> filter)
    {
        _sources.Add(async (s, t) =>
        {
            var raw = await s.Events.FetchStreamAsync(streamKey, token: t).ConfigureAwait(false);
            return raw.Where(filter).ToList();
        });
        return this;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. This member cannot propagate the annotation -- it implements a JasperFx interface that does not carry one, and annotating an implementation alone is IL2046. A CONSUMER is still warned, at the public entry point they call. ⚠️ This is NOT the suppression #733 removed: that one asserted the path was SAFE. This one records that the path is unsafe and that the diagnostic has nowhere to go from here. Upstream: annotate the JasperFx interface.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. This member cannot propagate the annotation -- it implements a JasperFx interface that does not carry one, and annotating an implementation alone is IL2046. A CONSUMER is still warned, at the public entry point they call. ⚠️ This is NOT the suppression #733 removed: that one asserted the path was SAFE. This one records that the path is unsafe and that the diagnostic has nowhere to go from here. Upstream: annotate the JasperFx interface.")]
    public IEventDataMasking IncludeEvents(Expression<Func<IEvent, bool>> filter)
    {
        _sources.Add((s, t) => s.Events.QueryAllRawEvents().Where(filter).ToListAsync(t));
        return this;
    }

    public IEventDataMasking AddHeader(string key, object value)
    {
        _headers[key] = value;
        return this;
    }

    public async Task ApplyAsync(CancellationToken token = default)
    {
        if (_sources.Count == 0)
            throw new InvalidOperationException(
                "You need to specify at least one stream identity or event filter first as part of the Fluent Interface");

        var session = BuildSession();

        foreach (var source in _sources)
        {
            var events = await source(session, token).ConfigureAwait(false);
            foreach (var @event in events)
            {
                if (_store.Events.TryMask(@event))
                {
                    foreach (var pair in _headers)
                    {
                        @event.Headers ??= new();
                        @event.Headers[pair.Key] = pair.Value;
                    }

                    session.Events.OverwriteEvent(@event);
                }
            }
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
    }

    internal IDocumentSession BuildSession()
    {
        if (string.IsNullOrEmpty(_tenantId))
            return _store.LightweightSession();

        return _store.LightweightSession(new SessionOptions { TenantId = _tenantId });
    }
}
