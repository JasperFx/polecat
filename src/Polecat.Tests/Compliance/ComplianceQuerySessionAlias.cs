// The shared compliance suites declare self-aggregating types whose EvolveAsync convention method
// takes the store's own read session. JasperFx's aggregate source generator resolves the parameter
// by type name, so a per-consumer global alias lets one shared source file bind to Polecat's
// IQuerySession here and to Marten's in Marten.
global using ComplianceQuerySession = Polecat.IQuerySession;
