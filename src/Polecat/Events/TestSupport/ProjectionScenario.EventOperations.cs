using JasperFx.Events;

namespace Polecat.Events.TestSupport;

public partial class ProjectionScenario
{
    /// <summary>
    ///     Append events to a stream by Guid id.
    /// </summary>
    public StreamAction Append(Guid stream, params object[] events)
    {
        var step = Action(e => e.Append(stream, events));
        step.Description = events.Length > 3
            ? $"Append({stream}, events)"
            : $"Append({stream}, {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Append(_store.Options.EventGraph, stream, events);
    }

    /// <summary>
    ///     Append events to a stream by string key.
    /// </summary>
    public StreamAction Append(string stream, params object[] events)
    {
        var step = Action(e => e.Append(stream, events));
        step.Description = events.Length > 3
            ? $"Append('{stream}', events)"
            : $"Append('{stream}', {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Append(_store.Options.EventGraph, stream, events);
    }

    /// <summary>
    ///     Append events to a stream by Guid id with expected version.
    /// </summary>
    public StreamAction Append(Guid stream, long expectedVersion, params object[] events)
    {
        var step = Action(e => e.Append(stream, expectedVersion, events));
        step.Description = events.Length > 3
            ? $"Append({stream}, {expectedVersion}, events)"
            : $"Append({stream}, {expectedVersion}, {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Append(_store.Options.EventGraph, stream, events);
    }

    /// <summary>
    ///     Append events to a stream by string key with expected version.
    /// </summary>
    public StreamAction Append(string stream, long expectedVersion, params object[] events)
    {
        var step = Action(e => e.Append(stream, expectedVersion, events));
        step.Description = events.Length > 3
            ? $"Append('{stream}', {expectedVersion}, events)"
            : $"Append('{stream}', {expectedVersion}, {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Append(_store.Options.EventGraph, stream, events);
    }

    /// <summary>
    ///     Start a new stream with a Guid id.
    /// </summary>
    public StreamAction StartStream(Guid id, params object[] events)
    {
        var step = Action(e => e.StartStream(id, events));
        step.Description = events.Length > 3
            ? $"StartStream({id}, events)"
            : $"StartStream({id}, {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Start(_store.Options.EventGraph, id, events);
    }

    /// <summary>
    ///     Start a new stream with a string key.
    /// </summary>
    public StreamAction StartStream(string streamKey, params object[] events)
    {
        var step = Action(e => e.StartStream(streamKey, events));
        step.Description = events.Length > 3
            ? $"StartStream('{streamKey}', events)"
            : $"StartStream('{streamKey}', {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Start(_store.Options.EventGraph, streamKey, events);
    }

    /// <summary>
    ///     Start a new stream with auto-generated Guid id.
    /// </summary>
    public StreamAction StartStream(params object[] events)
    {
        var streamId = Guid.NewGuid();
        var step = Action(e => e.StartStream(streamId, events));
        step.Description = events.Length > 3
            ? "StartStream(events)"
            : $"StartStream({string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Start(_store.Options.EventGraph, streamId, events);
    }

    /// <summary>
    ///     Start a new stream with a typed aggregate and Guid id.
    /// </summary>
    public StreamAction StartStream<TAggregate>(Guid id, params object[] events) where TAggregate : class
    {
        var step = Action(e => e.StartStream<TAggregate>(id, events));
        step.Description = events.Length > 3
            ? $"StartStream<{typeof(TAggregate).Name}>({id}, events)"
            : $"StartStream<{typeof(TAggregate).Name}>({id}, {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Start(_store.Options.EventGraph, id, events);
    }

    /// <summary>
    ///     Start a new stream with a typed aggregate and string key.
    /// </summary>
    public StreamAction StartStream<TAggregate>(string streamKey, params object[] events) where TAggregate : class
    {
        var step = Action(e => e.StartStream<TAggregate>(streamKey, events));
        step.Description = events.Length > 3
            ? $"StartStream<{typeof(TAggregate).Name}>('{streamKey}', events)"
            : $"StartStream<{typeof(TAggregate).Name}>('{streamKey}', {string.Join(", ", events.Select(x => x.ToString()))})";

        return StreamAction.Start(_store.Options.EventGraph, streamKey, events);
    }

    /// <summary>
    ///     Make any number of append event operations in the scenario sequence.
    /// </summary>
    public void AppendEvents(string description, Action<IEventOperations> appendAction)
    {
        Action(appendAction).Description = description;
    }

    /// <summary>
    ///     Make any number of append event operations in the scenario sequence.
    /// </summary>
    public void AppendEvents(Action<IEventOperations> appendAction)
    {
        AppendEvents("Appending events...", appendAction);
    }
}
