using Polecat.Projections.Flattened;

namespace JasperFx.Events.ComplianceTests;

/*
 * Polecat's half of the shared flat-table compliance projection.
 *
 * The compliance library owns the table name, the projection name and every event mapping; this
 * partial supplies the two things that cannot be portable. Flat-table projection bases take
 * constructor arguments describing where the table lives and those signatures genuinely differ
 * between products -- Polecat takes a literal schema name, Marten resolves one through a
 * SchemaNameSource enum -- and the column-declaration API hangs off each dialect's own Weasel Table
 * type, so the primary key cannot be declared portably either.
 *
 * The literal SchemaName rather than the store's schema: Polecat's base defaults to "dbo" when no
 * schema is passed, so the suite's constant is what keeps the projection writing where
 * QueryTableAsync looks. It is already lower case, which is what PolecatComplianceFixture
 * normalizes config.SchemaName to.
 */
public partial class ComplianceFlatTableProjection: FlatTableProjection
{
    public ComplianceFlatTableProjection(): base(TableName, SchemaName)
    {
        Table.AddColumn("id", "uniqueidentifier").AsPrimaryKey();

        ConfigureMappings();
    }
}
