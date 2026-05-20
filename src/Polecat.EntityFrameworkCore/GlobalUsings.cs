// Critter Stack 2026 dedupe pillar (jasperfx#214): TenancyStyle was lifted to
// JasperFx.MultiTenancy (#127 / jasperfx#327). This extension assembly references it
// unqualified in the EF Core projection storage wiring; alias it like the core
// Polecat assembly does in its own GlobalUsings.cs.
global using TenancyStyle = JasperFx.MultiTenancy.TenancyStyle;
