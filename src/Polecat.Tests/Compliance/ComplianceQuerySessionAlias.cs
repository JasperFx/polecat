// The shared compliance suites declare self-aggregating types whose EvolveAsync convention method
// takes the store's own read session. JasperFx's aggregate source generator resolves the parameter
// by type name, so a per-consumer global alias lets one shared source file bind to Polecat's
// IQuerySession here and to Marten's in Marten.
global using ComplianceQuerySession = Polecat.IQuerySession;

// Same mechanism for the EventProjection suites. Those declare projection types at file scope, so
// they cannot reach the <TOperations, TQuerySession> pair their suite class is generic over.
global using ComplianceOperations = Polecat.IDocumentSession;
global using ComplianceEventProjection = Polecat.Projections.EventProjection;

// The string-identity suite's custom projection needs Polecat's own SingleStreamProjection base, and
// that one is generic over the identity type as well as the document, so this alias names a closed
// generic rather than an open one.
global using ComplianceStringPartyProjectionBase =
    Polecat.Projections.SingleStreamProjection<JasperFx.Events.ComplianceTests.StringQuestParty, string>;
