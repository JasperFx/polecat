using Polecat.Metadata;

namespace Polecat;

/// <summary>
///     Configurable policies applied to document types.
/// </summary>
public class StorePolicies
{
    private readonly StoreOptions _parent;
    private bool _allDocumentsSoftDeleted;
    private readonly HashSet<Type> _softDeletedTypes = new();
    private readonly HashSet<Type> _tenantPartitioningDisabledTypes = new();
    private bool _allDocumentsAreMultiTenanted;
    private readonly HashSet<Type> _multiTenantedTypes = new();

    internal StorePolicies(StoreOptions parent)
    {
        _parent = parent;
    }

    /// <summary>
    ///     Enable soft deletes for all document types.
    /// </summary>
    public void AllDocumentsSoftDeleted()
    {
        _allDocumentsSoftDeleted = true;
    }

    /// <summary>
    ///     Make every document type conjoined multi-tenanted — a <c>tenant_id</c> column in the primary
    ///     key and every read scoped to the session's tenant — <b>without</b> physically partitioning
    ///     the tables (#682, jasperfx#898). Marten's <c>Policies.AllDocumentsAreMultiTenanted()</c>.
    /// </summary>
    /// <remarks>
    ///     The store-wide form of <c>Schema.For&lt;T&gt;().MultiTenanted()</c>. Note that unlike
    ///     <see cref="AllDocumentsAreMultiTenantedWithPartitioning" /> this does <b>not</b> touch
    ///     <c>Events.TenancyStyle</c>: document tenancy and event tenancy became independent axes in
    ///     #682, so a document-only store can be conjoined without pretending to have a conjoined
    ///     event store, and an event-sourced store can opt a reference document out.
    /// </remarks>
    public void AllDocumentsAreMultiTenanted()
    {
        _allDocumentsAreMultiTenanted = true;
    }

    /// <summary>
    ///     Make every document conjoined multi-tenanted AND physically partition every document table
    ///     per tenant through the store's shared managed tenant partitioning (#335 — the SQL Server
    ///     counterpart of Marten's <c>AllDocumentsAreMultiTenantedWithPartitioning</c> +
    ///     <c>PartitionMultiTenantedDocumentsUsingMartenManagement</c>). Sets
    ///     <c>Events.TenancyStyle = TenancyStyle.Conjoined</c> (document tenancy is store-wide in
    ///     Polecat) and adds a <c>tenant_ordinal</c> column + RANGE RIGHT partitioning to every
    ///     document table, driven by the one <c>pc_tenant_partitions</c> registry per database.
    ///     <para>
    ///     Tenants are onboarded lazily on first write, or explicitly (with per-table status
    ///     reporting and ordinal bucketing) through
    ///     <see cref="AdvancedOperations.AddPolecatManagedTenantsAsync(CancellationToken,string[])" />.
    ///     </para>
    /// </summary>
    public void AllDocumentsAreMultiTenantedWithPartitioning()
    {
        _parent.Events.TenancyStyle = TenancyStyle.Conjoined;
        DocumentTenantPartitioningEnabled = true;
    }

    /// <summary>
    ///     Physically partition every conjoined document table per tenant through the store's shared
    ///     managed tenant partitioning (#335), without changing the tenancy style — the store must
    ///     already be configured with <c>Events.TenancyStyle = TenancyStyle.Conjoined</c> (asserted at
    ///     store construction). The SQL Server counterpart of Marten's
    ///     <c>PartitionMultiTenantedDocumentsUsingMartenManagement</c>: SQL Server has no per-table
    ///     schema argument because the ordinal registry (<c>pc_tenant_partitions</c>) always lives in
    ///     the event store schema — one registry per database.
    /// </summary>
    public void PartitionMultiTenantedDocumentsUsingPolecatManagement()
    {
        DocumentTenantPartitioningEnabled = true;
    }

    /// <summary>
    ///     Enable soft deletes for a specific document type.
    /// </summary>
    public void ForDocument<T>(Action<DocumentPolicy> configure)
    {
        var policy = new DocumentPolicy();
        configure(policy);
        if (policy.SoftDeleted)
        {
            _softDeletedTypes.Add(typeof(T));
        }

        if (policy.DisableTenantPartitioning)
        {
            _tenantPartitioningDisabledTypes.Add(typeof(T));
        }

        if (policy.MultiTenanted)
        {
            _multiTenantedTypes.Add(typeof(T));
        }
    }

    internal bool IsSoftDeleted(Type documentType)
    {
        return _allDocumentsSoftDeleted || _softDeletedTypes.Contains(documentType);
    }

    /// <summary>
    ///     Whether this document type was declared conjoined multi-tenanted through a policy — the
    ///     store-wide <see cref="AllDocumentsAreMultiTenanted" /> or the per-type
    ///     <c>ForDocument&lt;T&gt;(p =&gt; p.MultiTenanted = true)</c>.
    /// </summary>
    /// <remarks>
    ///     Deliberately does NOT consider <c>Events.TenancyStyle</c>; <see cref="StoreOptions.TenancyStyleFor" />
    ///     is the single place that folds the policies together with the event store's style, so that
    ///     the fallback lives in one spot rather than in every caller.
    /// </remarks>
    internal bool IsMultiTenanted(Type documentType)
    {
        return _allDocumentsAreMultiTenanted || _multiTenantedTypes.Contains(documentType);
    }

    /// <summary>
    ///     True when any policy declares conjoined documents at all, for
    ///     <see cref="StoreOptions.HasAnyConjoinedDocuments" />'s store-wide question.
    /// </summary>
    internal bool AnyMultiTenantedDocuments => _allDocumentsAreMultiTenanted || _multiTenantedTypes.Count > 0;

    /// <summary>
    ///     True when the tenant-partitioned-documents policy is active for the store (#335).
    /// </summary>
    internal bool DocumentTenantPartitioningEnabled { get; private set; }

    /// <summary>
    ///     Whether this document type's table is managed-tenant-partitioned: the store-wide policy is
    ///     on and the type has not opted out via
    ///     <c>ForDocument&lt;T&gt;(p =&gt; p.DisableTenantPartitioning = true)</c>. The daemon's
    ///     dead-letter document is always excluded (Marten parity — its writes must never depend on
    ///     tenant onboarding, or a failing projection could dead-letter into a second failure).
    /// </summary>
    internal bool IsTenantPartitioned(Type documentType)
    {
        return DocumentTenantPartitioningEnabled
               && documentType != typeof(JasperFx.Events.Daemon.DeadLetterEvent)
               && !_tenantPartitioningDisabledTypes.Contains(documentType);
    }
}

/// <summary>
///     Per-document-type policy configuration.
/// </summary>
public class DocumentPolicy
{
    /// <summary>
    ///     Enable soft deletes for this document type.
    /// </summary>
    public bool SoftDeleted { get; set; }

    /// <summary>
    ///     Make this document type conjoined multi-tenanted (#682) — the policy-shaped twin of
    ///     <c>Schema.For&lt;T&gt;().MultiTenanted()</c>.
    /// </summary>
    public bool MultiTenanted { get; set; }

    /// <summary>
    ///     Opt this document type out of the store-wide managed tenant partitioning policy (#335) —
    ///     its table stays a plain conjoined table (<c>tenant_id</c> in the primary key, no physical
    ///     partitions). The Polecat analogue of Marten's <c>[SingleTenanted]</c> /
    ///     <c>DisablePartitioningIfAny</c> escape hatches.
    /// </summary>
    public bool DisableTenantPartitioning { get; set; }
}
