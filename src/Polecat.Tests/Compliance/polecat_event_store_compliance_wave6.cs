using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 5 part 2 (JasperFx 2.40.0) plus the strong-typed identity suite (2.42.0), enrolled together
 * because this repo took all four releases in one bump. Same shape as the earlier enrollment files
 * -- empty subclasses closing the shared suites over Polecat's session pair.
 */

public class flat_table_projection_compliance
    : FlatTableProjectionCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class string_stream_identity_compliance
    : StringStreamIdentityCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class multi_stream_projection_compliance
    : MultiStreamProjectionCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class snapshot_lifecycle_compliance
    : SnapshotLifecycleCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class strong_typed_identity_compliance
    : StrongTypedIdentityCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
