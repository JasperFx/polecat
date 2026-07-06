// Critter Stack 2026 dedupe pillar (jasperfx#214): types Polecat used to declare
// locally were lifted into JasperFx / JasperFx.Events. These global aliases keep
// every unqualified reference across Polecat resolving to the canonical lifted
// types (all ordinal-/shape-identical supersets), mirroring Marten's GlobalUsings.cs.
//
//   #127 (jasperfx#327): TenancyStyle -> JasperFx.MultiTenancy, DeleteStyle -> JasperFx
//   #128 (jasperfx#328): DcbConcurrencyException + ProgressionProgressOutOfOrderException -> JasperFx.Events(.Daemon)
//   #135 (jasperfx#335): IdentityAttribute -> JasperFx
//   #130 (jasperfx#330): ISoftDeleted / IVersioned / ITracked -> JasperFx.Metadata
//     (ITracked is non-nullable string upstream vs Polecat's string?; concrete
//      document classes keep their nullable annotations and still satisfy it.)
global using TenancyStyle = JasperFx.MultiTenancy.TenancyStyle;
global using DeleteStyle = JasperFx.DeleteStyle;
global using DcbConcurrencyException = JasperFx.Events.DcbConcurrencyException;
//   #140 (jasperfx#338): DocumentAlreadyExistsException -> JasperFx (message format now FullName-based)
global using DocumentAlreadyExistsException = JasperFx.DocumentAlreadyExistsException;
global using ProgressionProgressOutOfOrderException = JasperFx.Events.Daemon.ProgressionProgressOutOfOrderException;
global using IdentityAttribute = JasperFx.IdentityAttribute;
global using ISoftDeleted = JasperFx.Metadata.ISoftDeleted;
global using IVersioned = JasperFx.Metadata.IVersioned;
global using ITracked = JasperFx.Metadata.ITracked;
//   #131 (jasperfx#331): IPatchExpression<T> (Marten superset) + RemoveAction -> JasperFx.Events
global using RemoveAction = JasperFx.Events.RemoveAction;
//   #132 (jasperfx#332): TrackLevel -> JasperFx.OpenTelemetry (OpenTelemetryOptions base subclassed in-place)
global using TrackLevel = JasperFx.OpenTelemetry.TrackLevel;
//   #137 (weasel#287): Hi-Lo sequence contract + settings lifted to Weasel.Core.Sequences.
//     HiloSequence now derives from Weasel.Core.Sequences.HiloSequenceBase; these aliases
//     keep SequenceFactory / StoreOptions / DocumentMapping / AdvancedOperations resolving
//     ISequence / IReadOnlyHiloSettings / HiloSettings by their old unqualified names.
global using ISequence = Weasel.Core.Sequences.ISequence;
global using IReadOnlyHiloSettings = Weasel.Core.Sequences.IReadOnlyHiloSettings;
global using HiloSettings = Weasel.Core.Sequences.HiloSettings;
//   #273: Polecat.Serialization.ISerializer now extends the shared Weasel.Core.ISerializer base.
//     Alias the unqualified name to Polecat's richer subtype so files importing both namespaces
//     keep resolving ISerializer to Polecat's (which carries the string-based overloads too).
global using ISerializer = Polecat.Serialization.ISerializer;
//   #273 / Weasel 9.7.0 (weasel#327/#328): a shared dialect-neutral ICommandBuilder was added to
//     Weasel.Core, and Weasel.SqlServer.ICommandBuilder now derives from it. Polecat's operations
//     bind SqlParameters, so the unqualified name keeps resolving to the SqlServer subtype.
global using ICommandBuilder = Weasel.SqlServer.ICommandBuilder;
