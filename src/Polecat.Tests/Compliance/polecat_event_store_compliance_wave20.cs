using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 20 -- JasperFx.Events.ComplianceTests 2.93.0 (#813).
 *
 * Three new suites, plus fourteen new facts on DocumentStoreDiagnosticsCompliance (already enrolled in
 * the base file) that go live as their gates flip in the fixtures:
 *
 *   - SoftDeleteCompliance (jasperfx#785): implicit exclusion, the four soft-delete markers, DeleteWhere
 *     soft-deleting, UndoDeleteWhere / HardDeleteWhere, and the refusal of a marker against a type that
 *     is not soft-deleted. That refusal is the fact #813 expected to be red until #558; #558 had already
 *     landed, so it is green on arrival.
 *   - DocumentMetadataCompliance (jasperfx#785): ITracked stamping from the session, and the created /
 *     last-modified timestamps as the diagnostics surface reports them.
 *   - ProjectionFloorCompliance (jasperfx#809): persisted start floors over pc_event_projection_floors,
 *     which is what makes AsyncOptions.SubscribeFromFirstRun() work on Polecat.
 *
 * The diagnostics facts are jasperfx#1058 (OrderByMetadata) and jasperfx#1059 (UndeleteDocumentAsync).
 */

public class polecat_soft_delete_compliance
    : SoftDeleteCompliance<PolecatDocumentComplianceFixture>;

public class polecat_document_metadata_compliance
    : DocumentMetadataCompliance<PolecatDocumentComplianceFixture>;

public class polecat_projection_floor_compliance
    : ProjectionFloorCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
