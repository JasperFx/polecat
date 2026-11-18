using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 23 -- JasperFx.Events.ComplianceTests 3.0.0 (polecat#806, jasperfx#1049).
 *
 *   - ProjectionStepThroughCompliance: RunProjectionAsync / RunProjectionByNameAsync over rows the store
 *     really wrote. DocumentStore.ProjectionReplay now forwards to the shared ProjectionStepThrough, which
 *     records each step as a detached copy read back through the store serializer. Polecat's own replay
 *     recorded live references, so an aggregate mutated in place showed its final state on every step.
 */

public class polecat_projection_step_through_compliance
    : ProjectionStepThroughCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
