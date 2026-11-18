using JasperFx.Core;
using Weasel.Core;
using Weasel.SqlServer.Tables;

namespace Polecat.Storage;

/// <summary>
///     Schema objects of your own that the store manages beside its own, through
///     <see cref="StoreOptions.Storage" />. The same shape as Marten's <c>StoreOptions.Storage</c>.
/// </summary>
public class StorageFeatures
{
    private readonly StoreOptions _options;
    private readonly List<Table> _tablesPartitionedPerTenant = new();

    internal StorageFeatures(StoreOptions options)
    {
        _options = options;
    }

    /// <summary>
    ///     Additional SQL Server tables, functions, or sequences to be managed by this store. The same list as
    ///     <see cref="StoreOptions.ExtendedSchemaObjects" />.
    /// </summary>
    public List<ISchemaObject> ExtendedSchemaObjects => _options.ExtendedSchemaObjects;

    /// <summary>
    ///     The tables registered with <see cref="PartitionPerTenant" />.
    /// </summary>
    internal IReadOnlyList<Table> TablesPartitionedPerTenant => _tablesPartitionedPerTenant;

    /// <summary>
    ///     Partition one of your own tables per tenant, the same way Polecat partitions its document and event
    ///     tables under Polecat-managed tenant partitioning: on the store's shared tenant partition scheme, by
    ///     the tenant's partition ordinal in an <c>int</c> column named <c>tenant_ordinal</c>. Each tenant gets
    ///     its partition in your table when it is added, and schema migrations leave the tenants' partitions
    ///     alone, so re-applying the schema to a database that already has tenants never rebuilds the table.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The table gets a <c>tenant_ordinal int NOT NULL</c> column if it doesn't have one, and the column
    ///         joins its primary key, because SQL Server requires the partition column in the clustered index.
    ///         The table is added to <see cref="ExtendedSchemaObjects" /> if it is not there already.
    ///     </para>
    ///     <para>
    ///         Your inserts write the tenant's ordinal into <c>tenant_ordinal</c>. Get it from
    ///         <c>store.Advanced.TenantOrdinalAsync(tenantId)</c>, or in SQL from the store's
    ///         <c>{schema}.pc_tenant_ordinal(@tenant_id)</c> function.
    ///     </para>
    ///     <para>
    ///         The store must use Polecat-managed tenant partitioning
    ///         (<c>Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement()</c> or
    ///         <c>Events.UseTenantPartitionedEvents</c>), otherwise building the store throws.
    ///     </para>
    /// </remarks>
    /// <param name="table">A table of your own</param>
    public void PartitionPerTenant(Table table)
    {
        ArgumentNullException.ThrowIfNull(table);

        if (!_tablesPartitionedPerTenant.Contains(table))
        {
            _tablesPartitionedPerTenant.Add(table);
        }

        if (!ExtendedSchemaObjects.Contains(table))
        {
            ExtendedSchemaObjects.Add(table);
        }
    }

    /// <summary>
    ///     #1057: applied when the store is built, once every way of turning on tenant partitioning has been
    ///     configured.
    /// </summary>
    internal void ApplyTenantPartitioning()
    {
        if (_tablesPartitionedPerTenant.Count == 0) return;

        var events = _options.EventGraph;
        if (!events.AnyTenantPartitioning)
        {
            var tables = _tablesPartitionedPerTenant.Select(x => x.Identifier.QualifiedName).Join(", ");
            throw new InvalidOperationException(
                $"Storage.PartitionPerTenant() was called for {tables}, but this store does not use Polecat-managed "
                + "tenant partitioning. Turn it on with "
                + $"{nameof(StoreOptions.Policies)}.{nameof(StorePolicies.PartitionMultiTenantedDocumentsUsingPolecatManagement)}() "
                + "or Events.UseTenantPartitionedEvents.");
        }

        var manager = events.TenantPartitionManager;
        foreach (var table in _tablesPartitionedPerTenant)
        {
            if (ReferenceEquals(table.SqlServerPartitioning, manager)) continue;

            if (table.SqlServerPartitioning != null)
            {
                throw new InvalidOperationException(
                    $"Storage.PartitionPerTenant() was called for {table.Identifier.QualifiedName}, which is already "
                    + "partitioned another way. A SQL Server table has only one partition scheme.");
            }

            var column = table.ColumnFor(manager.Column);
            if (column == null)
            {
                table.AddColumn(manager.Column, "int").NotNull();
                column = table.ColumnFor(manager.Column)!;
            }

            column.AllowNulls = false;

            if (table.PrimaryKeyColumns.Count > 0 && !column.IsPrimaryKey)
            {
                table.ModifyColumn(manager.Column).AsPrimaryKey();
            }

            table.PartitionByManagedTenants(manager);
        }
    }
}
