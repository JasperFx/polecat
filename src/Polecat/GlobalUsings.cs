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
global using ProgressionProgressOutOfOrderException = JasperFx.Events.Daemon.ProgressionProgressOutOfOrderException;
global using IdentityAttribute = JasperFx.IdentityAttribute;
global using ISoftDeleted = JasperFx.Metadata.ISoftDeleted;
global using IVersioned = JasperFx.Metadata.IVersioned;
global using ITracked = JasperFx.Metadata.ITracked;
//   #131 (jasperfx#331): IPatchExpression<T> (Marten superset) + RemoveAction -> JasperFx.Events
global using RemoveAction = JasperFx.Events.RemoveAction;
//   #132 (jasperfx#332): TrackLevel -> JasperFx.OpenTelemetry (OpenTelemetryOptions base subclassed in-place)
global using TrackLevel = JasperFx.OpenTelemetry.TrackLevel;
