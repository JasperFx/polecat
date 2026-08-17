using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 9 -- the two suites JasperFx 2.50.0 added, both opt-in rather than baseline, and Polecat
 * enrolls in both.
 *
 * BinaryEventSerializationCompliance is the definition of done for #475 / #471: per-event-type opt
 * out of the JSON path, with the shared JasperFx.Events.IEventBinarySerializer as the contract so a
 * consumer compiling one body of source against several stores writes ONE serializer. Polecat's
 * storage half predates the promotion (#388 landed the bdata column and the row-level
 * discriminator); what #475 changed is that the seam is now the shared interface and the shared
 * [BinaryEvent], which is what makes this suite runnable at all.
 *
 * DocumentSessionEventsCompliance is jasperfx#669: the route from a session a consumer opened
 * through IDocumentSessionFactory to that session's event store. It catches a failure that is
 * otherwise SILENT -- C# interface implementation is not return-type covariant, so Polecat's
 * sessions declaring Events as Polecat.Events.IQueryEventStore / Polecat.Events.IEventOperations did
 * not satisfy the contract members and bound to their throwing defaults with no compile error
 * anywhere. See the explicit implementations on IQuerySession and IDocumentSession.
 */

public class binary_event_serialization_compliance
    : BinaryEventSerializationCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class polecat_document_session_events_compliance
    : DocumentSessionEventsCompliance<PolecatDocumentComplianceFixture>;
