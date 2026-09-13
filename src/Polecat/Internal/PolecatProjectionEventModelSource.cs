using JasperFx;
using JasperFx.Events;
using JasperFx.Events.EventModeling;
using Microsoft.Extensions.DependencyInjection;

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

    /// <summary>
    ///     The model this store's slices contribute to: the configured name, else the service name,
    ///     else the shared literal.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>The service name rather than "EventModel" (gh-623).</b>
    ///         <c>ProjectionEventModelSource.DefaultModelName</c> is the literal <c>EventModel</c>,
    ///         which is the one default guaranteed to be wrong for every host — and it was the whole
    ///         cause of gh-615. Every other contributor already defaults to something meaningful:
    ///         Wolverine's chains and HTTP use the service name, a Bobcat spec assembly uses the
    ///         assembly name, a curated file uses its own <c>model:</c> value. A store defaulting to
    ///         a literal is what made the commonest host of all — Wolverine plus one store — assemble
    ///         TWO models out of the box, and made every host restate a name it had already declared.
    ///     </para>
    ///     <para>
    ///         An explicit <see cref="StoreOptions.EventModelName" /> still wins, which is what a
    ///         modular monolith needs when a module's store really is its own bounded context. The
    ///         literal survives only for a host with no JasperFx options at all.
    ///     </para>
    ///     <para>
    ///         Read here rather than captured at registration, for the same reason the configured
    ///         name is: <c>JasperFxOptions</c> is a singleton whose <c>ServiceName</c> a host may set
    ///         after this source was registered.
    ///     </para>
    /// </remarks>
    private static string ModelNameFor(IDocumentStore store, IServiceProvider services)
        => store.Options.EventModelName
           ?? services.GetService<JasperFxOptions>()?.ServiceName
           ?? ProjectionEventModelSource.DefaultModelName;

    public Task<EventModelDescriptor?> TryCreateAsync(IServiceProvider services, CancellationToken token)
    {
        var store = _store(services);

        var inner = new ProjectionEventModelSource(_ => [(IEventStore)store])
        {
            ModelName = ModelNameFor(store, services),
            Subject = Subject
        };

        return inner.TryCreateAsync(services, token);
    }
}
