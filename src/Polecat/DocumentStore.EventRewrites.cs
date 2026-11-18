using JasperFx.Events;
using Polecat.Events.Rewriting;
using Polecat.Storage;

namespace Polecat;

// jasperfx#1215: the stored-event rewrite seam a Wolverine job drives, over Advanced.RewriteStoredEventsAsync (#1034)
public partial class DocumentStore
{
    /// <summary>
    ///     True unless the event-range marks are turned off (<see cref="EventStoreOptions.EnableEventRangeMarks" />),
    ///     because the rewrite keeps its cursor in them.
    /// </summary>
    bool IEventStore.SupportsEventRewrites => Options.Events.EnableEventRangeMarks;

    Task<IReadOnlyList<EventRewriteProgress>> IEventStore.RewriteStoredEventsAsync(EventRewriteRun run,
        CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(run);

        // Refuses an unknown transform before anything is read
        var transforms = Options.EventGraph.FindRewriteTransforms(run.Definition);

        if (run.Database is not PolecatDatabase database)
        {
            throw new ArgumentOutOfRangeException(nameof(run),
                $"{run.Database.GetType().FullName} is not a Polecat database, so this store cannot rewrite its events.");
        }

        var rewrite = StoredEventRewriter.RewriteFor(Options.EventGraph, run.Definition, transforms);
        return new StoredEventRewriter(this, rewrite).ExecuteRunAsync(database, run, token);
    }
}
