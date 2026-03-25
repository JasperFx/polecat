using JasperFx.Events;
using Polecat.Projections;

namespace Polecat.Tests.Projections;

#region sample_polecat_event_projection_flat_table_events

public record ImportStarted(
    DateTimeOffset Started,
    string ActivityType,
    string CustomerId,
    int PlannedSteps);

public record ImportProgress(
    string StepName,
    int Records,
    int Invalids);

public record ImportFinished(DateTimeOffset Finished);

public record ImportFailed;

#endregion

#region sample_polecat_import_sql_event_projection

public partial class ImportSqlProjection: EventProjection
{
    // Use the IEvent<T> envelope to access event metadata
    // like stream identity and timestamps
    public void Project(IEvent<ImportStarted> e, IDocumentSession ops)
    {
        ops.QueueSqlCommand(
            "insert into import_history (id, activity_type, customer_id, started) values (?, ?, ?, ?)",
            e.StreamId, e.Data.ActivityType, e.Data.CustomerId, e.Data.Started
        );
    }

    public void Project(IEvent<ImportFinished> e, IDocumentSession ops)
    {
        ops.QueueSqlCommand(
            "update import_history set finished = ? where id = ?",
            e.Data.Finished, e.StreamId
        );
    }

    // You can use any SQL operation, including deletes
    public void Project(IEvent<ImportFailed> e, IDocumentSession ops)
    {
        ops.QueueSqlCommand(
            "delete from import_history where id = ?",
            e.StreamId
        );
    }
}

#endregion
