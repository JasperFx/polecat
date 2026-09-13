using JasperFx.Events;
using JasperFx.Events.EventModeling;

namespace Polecat.Internal;

/// <summary>
///     The store-derived Event Model rung for one Polecat store, reading its model name from
///     <see cref="StoreOptions.EventModelName" /> when the model is assembled.
/// </summary>
/// <remarks>
///     <para>
///         <b>Why this exists rather than a plain <c>AddProjectionEventModelSource(stores, name)</c>
///         (gh-618).</b> That helper takes the name at REGISTRATION time, which forces it to be a
///         parameter of <c>AddPolecat</c> — and a name captured then cannot see a configuration pass
///         that runs later, <c>IConfigurePolecat</c> included. Resolving the store inside
///         <see cref="TryCreateAsync" /> and reading its options there means the last word on the
///         name belongs to the options, wherever they were configured.
///     </para>
///     <para>
///         Everything else is delegated to JasperFx's <see cref="ProjectionEventModelSource" />, which
///         is what actually reads the projections. This type contributes the name and the subject and
///         nothing else — a store-specific wrapper, not a reimplementation.
///     </para>
/// </remarks>
internal sealed class PolecatProjectionEventModelSource: IEventModelDefinitionSource
{
    private readonly Func<IServiceProvider, IDocumentStore> _store;

    public PolecatProjectionEventModelSource(Func<IServiceProvider, IDocumentStore> store, Uri subject)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        Subject = subject ?? throw new ArgumentNullException(nameof(subject));
    }

    public Uri Subject { get; }

    /// <summary>
    ///     <see cref="EventModelProvenance.Derived" /> — these roles are read out of the store's own
    ///     registrations, not declared by a human.
    /// </summary>
    public EventModelProvenance Provenance => EventModelProvenance.Derived;

    public Task<EventModelDescriptor?> TryCreateAsync(IServiceProvider services, CancellationToken token)
    {
        var store = _store(services);

        var inner = new ProjectionEventModelSource(_ => [(IEventStore)store])
        {
            ModelName = store.Options.EventModelName ?? ProjectionEventModelSource.DefaultModelName,
            Subject = Subject
        };

        return inner.TryCreateAsync(services, token);
    }
}
