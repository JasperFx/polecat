using Weasel.SqlServer;

namespace Polecat.Storage;

/// <summary>
///     Weasel table definition for <c>pc_tenants</c>, the control-plane table
///     <see cref="MasterTableTenancy" /> routes tenants from.
/// </summary>
/// <remarks>
///     #883: this was a guarded CREATE TABLE string. It has to stay the same shape as the tables that string
///     already made, down to the primary key's name, so that their first delta comes out <c>None</c>.
/// </remarks>
internal class TenantsTable : AdditiveTable
{
    public TenantsTable(string schemaName)
        : base(new SqlServerObjectName(schemaName, MasterTableTenancy.TableName))
    {
        AddColumn("tenant_id", "nvarchar(200)").AsPrimaryKey().NotNull();
        AddColumn("connection_string", "nvarchar(max)").NotNull();
        AddColumn("is_disabled", "bit").NotNull().DefaultValue(0);

        PrimaryKeyName = "pk_pc_tenants";
    }
}
