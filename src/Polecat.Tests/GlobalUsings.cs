// Critter Stack 2026 dedupe pillar (jasperfx#214): mirror src/Polecat/GlobalUsings.cs
// so test code keeps referencing the lifted types by their old unqualified names.
// These were lifted out of Polecat into JasperFx / JasperFx.Events (#127/#128/#135);
// the aliases below cover every name the test assembly still uses unqualified.
global using TenancyStyle = JasperFx.MultiTenancy.TenancyStyle;
global using DeleteStyle = JasperFx.DeleteStyle;
global using DcbConcurrencyException = JasperFx.Events.DcbConcurrencyException;
global using ProgressionProgressOutOfOrderException = JasperFx.Events.Daemon.ProgressionProgressOutOfOrderException;
global using ISoftDeleted = JasperFx.Metadata.ISoftDeleted;
global using IVersioned = JasperFx.Metadata.IVersioned;
global using ITracked = JasperFx.Metadata.ITracked;
