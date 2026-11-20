using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 27 -- JasperFx.Events.ComplianceTests 3.10.0 (marten#6003, polecat#1053).
 *
 *   - ParkedIdentityCompliance: a failing identity of an async single-stream projection parks in
 *     pc_parked_identities inside the batch, its later events are held, and a replay or discard unparks it.
 *   - MultiStreamParkedIdentityCompliance: the same for multi-stream projections grouped by stream, by DCB tag, or
 *     through the slice index (pc_slice_index_*), plus the per-identity rebuild and the reads behind the stepper.
 */

public class parked_identity_compliance
    : ParkedIdentityCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class multi_stream_parked_identity_compliance
    : MultiStreamParkedIdentityCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
