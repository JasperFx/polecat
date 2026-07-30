using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Weasel.Core.Partitioning;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Storage;

/// <summary>
///     Fluent configuration builder for a document type's mapping.
///     Used via StoreOptions.Schema.For&lt;T&gt;().
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: AddSubClassHierarchy uses Assembly.GetTypes() to discover subclasses of T. Document hierarchies are part of the registered surface and AOT consumers must preserve subclass types (JsonSerializerContext / per-type registration) per the AOT publishing guide.")]
public class DocumentMappingExpression<T>
{
    internal readonly Type DocumentType = typeof(T);
    internal readonly List<(Type SubClass, string? Alias)> SubClasses = new();
    internal readonly List<DocumentIndex> Indexes = new();
    internal readonly List<JsonIndex> JsonIndexes = new();
    internal readonly List<DocumentForeignKey> ForeignKeys = new();
    internal DocumentPartitioning? Partitioning;
    internal readonly Metadata.DocumentMetadataConfig MetadataConfig = new();

    /// <summary>
    ///     #243: configure document metadata columns — enable opt-in columns
    ///     (correlation/causation/last-modified-by/headers) and/or map any stored metadata value
    ///     onto a document member. Mirrors Marten's <c>Schema.For&lt;T&gt;().Metadata(m =&gt; ...)</c>.
    /// </summary>
    public DocumentMappingExpression<T> Metadata(Action<Metadata.MetadataConfig<T>> configure)
    {
        configure(new Metadata.MetadataConfig<T>(MetadataConfig));
        return this;
    }

    /// <summary>
    ///     Register a subclass of T for document hierarchy (single-table inheritance).
    ///     Subclass documents are stored in the same table as T with a doc_type discriminator column.
    /// </summary>
    public DocumentMappingExpression<T> AddSubClass<TSubClass>(string? alias = null) where TSubClass : T
    {
        SubClasses.Add((typeof(TSubClass), alias));
        return this;
    }

    /// <summary>
    ///     Register a subclass by type for document hierarchy.
    /// </summary>
    public DocumentMappingExpression<T> AddSubClass(Type subclassType, string? alias = null)
    {
        SubClasses.Add((subclassType, alias));
        return this;
    }

    /// <summary>
    ///     Auto-discover and register all subclasses of T in T's assembly.
    /// </summary>
    public DocumentMappingExpression<T> AddSubClassHierarchy()
    {
        var assembly = typeof(T).Assembly;
        foreach (var type in assembly.GetTypes())
        {
            if (type != typeof(T) && typeof(T).IsAssignableFrom(type) && !type.IsAbstract)
            {
                SubClasses.Add((type, null));
            }
        }
        return this;
    }

    /// <summary>
    ///     Add a computed index on one or more document properties, mirroring Marten's
    ///     <c>Schema.For&lt;T&gt;().Index(...)</c>. Properties are extracted via JSON_VALUE from the
    ///     data column into persisted computed columns. Pass <paramref name="include" /> (a member or
    ///     anonymous type) to carry those members as non-key <c>INCLUDE</c> columns for a covering
    ///     index that avoids key lookups — the type-safe equivalent of setting
    ///     <see cref="DocumentIndex.IncludeColumns" /> inside <paramref name="configure" />.
    /// </summary>
    public DocumentMappingExpression<T> Index(Expression<Func<T, object?>> expression,
        Action<DocumentIndex>? configure = null, Expression<Func<T, object?>>? include = null)
    {
        var paths = DocumentIndex.ResolveJsonPaths(expression);
        var index = new DocumentIndex(paths);
        if (include != null) index.IncludeColumns = DocumentIndex.ResolveJsonPaths(include);
        configure?.Invoke(index);
        Indexes.Add(index);
        return this;
    }

    /// <summary>
    ///     Add a unique index on one or more document properties, mirroring Marten's
    ///     <c>Schema.For&lt;T&gt;().UniqueIndex(...)</c>. Pass <paramref name="include" /> to carry
    ///     extra members as non-key <c>INCLUDE</c> columns (covering index).
    /// </summary>
    public DocumentMappingExpression<T> UniqueIndex(Expression<Func<T, object?>> expression,
        Action<DocumentIndex>? configure = null, Expression<Func<T, object?>>? include = null)
    {
        var paths = DocumentIndex.ResolveJsonPaths(expression);
        var index = new DocumentIndex(paths) { IsUnique = true };
        if (include != null) index.IncludeColumns = DocumentIndex.ResolveJsonPaths(include);
        configure?.Invoke(index);
        Indexes.Add(index);
        return this;
    }

    /// <summary>
    ///     Add a native SQL Server 2025 JSON index (<c>CREATE JSON INDEX</c>) over one or more JSON
    ///     paths in the document. A single JSON index covers all the given paths and accelerates
    ///     <c>JSON_VALUE</c> (=, including the <c>RETURNING</c> form) / <c>JSON_PATH_EXISTS</c> /
    ///     <c>JSON_CONTAINS</c> predicates without per-path computed columns. Requires
    ///     <c>UseNativeJsonType = true</c>. See <see cref="JsonIndex" /> for the constraints.
    /// </summary>
    public DocumentMappingExpression<T> JsonIndex(Expression<Func<T, object?>> expression,
        Action<JsonIndex>? configure = null)
    {
        var paths = Storage.JsonIndex.ResolveJsonPaths(expression);
        var index = new JsonIndex(paths);
        configure?.Invoke(index);
        JsonIndexes.Add(index);
        return this;
    }

    /// <summary>
    ///     Add a native JSON index over the entire JSON document (no <c>FOR</c> path filter) — the
    ///     SQL Server counterpart to Marten's <c>GinIndexJsonData</c>. Requires
    ///     <c>UseNativeJsonType = true</c>.
    /// </summary>
    public DocumentMappingExpression<T> JsonIndex(Action<JsonIndex>? configure = null)
    {
        var index = new JsonIndex([]);
        configure?.Invoke(index);
        JsonIndexes.Add(index);
        return this;
    }

    /// <summary>
    ///     Add a custom index with explicit configuration.
    /// </summary>
    public DocumentMappingExpression<T> AddIndex(DocumentIndex index)
    {
        Indexes.Add(index);
        return this;
    }

    /// <summary>
    ///     Add a foreign key from a document property to another document type's table.
    /// </summary>
    public DocumentMappingExpression<T> ForeignKey<TReference>(Expression<Func<T, object?>> expression,
        Action<DocumentForeignKey>? configure = null)
    {
        var path = DocumentForeignKey.ResolveJsonPath(expression);
        var fk = new DocumentForeignKey(path, typeof(TReference));
        configure?.Invoke(fk);
        ForeignKeys.Add(fk);
        return this;
    }

    /// <summary>
    ///     Add a foreign key with explicit configuration.
    /// </summary>
    public DocumentMappingExpression<T> AddForeignKey(DocumentForeignKey foreignKey)
    {
        ForeignKeys.Add(foreignKey);
        return this;
    }

    /// <summary>
    ///     Declaratively RANGE-partition this document's table on a member — the SQL Server companion to
    ///     Marten's <c>PartitionOn</c>. The classic use is a date member (e.g. <c>x =&gt; x.BucketEnd</c>)
    ///     partitioned monthly so old data can be pruned by dropping a partition. The boundaries are the
    ///     RANGE RIGHT split points (N boundaries → N+1 partitions); add new boundaries over time and
    ///     Weasel rolls them forward in place via <c>SPLIT RANGE</c>.
    /// </summary>
    /// <remarks>
    ///     Unless the member is the identity, its value is promoted into a real column written on every
    ///     upsert and added to the primary key (SQL Server requires the partition column in the table's
    ///     unique index). Currently supported for single-tenant document tables only.
    /// </remarks>
    public DocumentMappingExpression<T> PartitionByRange<TValue>(
        Expression<Func<T, TValue>> member,
        params TValue[] boundaries)
    {
        var idMemberName = DocumentMapping.FindIdProperty(typeof(T))?.Name ?? "Id";
        Partitioning = DocumentPartitioning.For(member, boundaries, idMemberName);
        return this;
    }

    /// <summary>
    ///     #255: begin a fluent declaration of RANGE partitioning on a member, mirroring Marten's
    ///     <c>PartitionOn(x =&gt; x.Member)</c>. Follow with:
    ///     <list type="bullet">
    ///         <item><see cref="PartitioningExpression{T,TValue}.ByRange" /> — a fixed set of boundaries
    ///         Polecat owns and rolls forward in place as you add to the list;</item>
    ///         <item><see cref="PartitioningExpression{T,TValue}.ByRollingRange(PartitionPeriod,int,int,TimeProvider)" />
    ///         — a rolling time window Polecat provisions ahead of and retires behind the clock, which is
    ///         the supported way to run a time-series table (#386);</item>
    ///         <item><see cref="PartitioningExpression{T,TValue}.ByExternallyManagedRange" /> — Polecat
    ///         provisions the partitioned table once and never touches the partitions again, for when
    ///         something genuinely outside Polecat owns them.</item>
    ///     </list>
    /// </summary>
    public PartitioningExpression<T, TValue> PartitionOn<TValue>(Expression<Func<T, TValue>> member)
    {
        return new PartitioningExpression<T, TValue>(this, member);
    }

    /// <summary>Internal hook used by <see cref="PartitioningExpression{T,TValue}" /> to set the descriptor.</summary>
    internal void SetPartitioning<TValue>(Expression<Func<T, TValue>> member, TValue[] boundaries,
        bool externallyManaged)
    {
        Partitioning = DocumentPartitioning.For(member, boundaries, IdMemberName(), externallyManaged);
    }

    /// <summary>
    ///     #386: internal hook used by <see cref="PartitioningExpression{T,TValue}" /> to set a
    ///     rolling-time-window descriptor. Returns the manager that owns the window.
    /// </summary>
    internal ManagedRangePartitions SetRollingWindow<TValue>(Expression<Func<T, TValue>> member,
        RollingWindowPolicy policy, TimeProvider? timeProvider, ManagedRangePartitions? prebuilt)
    {
        Partitioning = DocumentPartitioning.ForRollingWindow(member, IdMemberName(), policy, timeProvider,
            prebuilt);

        return Partitioning.RollingWindow!;
    }

    private static string IdMemberName() => DocumentMapping.FindIdProperty(typeof(T))?.Name ?? "Id";
}

/// <summary>
///     #255: fluent continuation of <see cref="DocumentMappingExpression{T}.PartitionOn{TValue}" />,
///     mirroring Marten's <c>PartitioningExpression</c>. Choose the range-partitioning strategy.
/// </summary>
public class PartitioningExpression<T, TValue>
{
    private readonly DocumentMappingExpression<T> _parent;
    private readonly Expression<Func<T, TValue>> _member;

    internal PartitioningExpression(DocumentMappingExpression<T> parent, Expression<Func<T, TValue>> member)
    {
        _parent = parent;
        _member = member;
    }

    /// <summary>
    ///     Polecat-managed RANGE partitioning: the boundaries are owned by Polecat and rolled forward
    ///     in place via <c>SPLIT RANGE</c> when you add new ones. N boundaries → N+1 partitions.
    /// </summary>
    public DocumentMappingExpression<T> ByRange(params TValue[] boundaries)
    {
        _parent.SetPartitioning(_member, boundaries, externallyManaged: false);
        return _parent;
    }

    /// <summary>
    ///     #386: RANGE-partition the table over a <em>rolling time window</em> that Polecat itself owns —
    ///     it provisions the periods at the leading edge and retires the aged ones at the trailing edge
    ///     on the same schedule it applies every other schema change. This is the supported way to run a
    ///     time-series document table: retention becomes a partition <c>TRUNCATE</c> + <c>MERGE RANGE</c>
    ///     (an O(1) page deallocation, not a mass <c>DELETE</c>) without giving up Weasel's schema
    ///     ordering and dependency management the way <see cref="ByExternallyManagedRange" /> does.
    ///     <para>
    ///         The partitioned member must be a <c>DateTime</c> or <c>DateTimeOffset</c>, and the whole
    ///         window is computed in UTC. Polecat promotes the member into a real column and adds it to
    ///         the primary key, as SQL Server requires of a partitioned table's unique index.
    ///     </para>
    /// </summary>
    /// <param name="period">The size of a single partition — hour, day, week, month or year.</param>
    /// <param name="periodsAhead">
    ///     How many periods beyond the current one to provision. At least one is strongly recommended so
    ///     rows written at the very end of a period always have a partition waiting for them.
    /// </param>
    /// <param name="periodsBehind">
    ///     How many completed periods to retain. Periods older than this are retired by the retention
    ///     pass, which destroys their rows by design.
    /// </param>
    /// <param name="timeProvider">
    ///     Clock used to resolve "now". Defaults to <see cref="TimeProvider.System" />; supply a fake to
    ///     roll the window forward deterministically in tests.
    /// </param>
    /// <returns>
    ///     The manager that owns the window, so <c>Filegroup</c> can be set or the same instance shared
    ///     with another document type.
    /// </returns>
    /// <seealso href="https://github.com/JasperFx/polecat/issues/386" />
    public ManagedRangePartitions ByRollingRange(PartitionPeriod period, int periodsAhead, int periodsBehind,
        TimeProvider? timeProvider = null)
        => ByRollingRange(new RollingWindowPolicy(period, periodsAhead, periodsBehind), timeProvider);

    /// <summary>
    ///     #386: RANGE-partition the table over the rolling time window described by
    ///     <paramref name="policy" />. See
    ///     <see cref="ByRollingRange(PartitionPeriod,int,int,TimeProvider)" />.
    /// </summary>
    public ManagedRangePartitions ByRollingRange(RollingWindowPolicy policy, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(policy);

        return _parent.SetRollingWindow(_member, policy, timeProvider, prebuilt: null);
    }

    /// <summary>
    ///     #386: RANGE-partition the table over a rolling time window owned by a pre-built
    ///     <see cref="ManagedRangePartitions" />. Pass the <em>same</em> manager instance to several
    ///     document types to roll every one of their tables forward in a single pass. The manager's
    ///     column and SQL data type must match what the partition member resolves to.
    /// </summary>
    public ManagedRangePartitions ByRollingRange(ManagedRangePartitions partitions)
    {
        ArgumentNullException.ThrowIfNull(partitions);

        return _parent.SetRollingWindow(_member, partitions.Policy, timeProvider: null, partitions);
    }

    /// <summary>
    ///     #255: externally-managed RANGE partitioning: Polecat creates the partition function/scheme
    ///     and table once (with the supplied <paramref name="initialBoundaries" />) and then never
    ///     reconciles the partitioning, so whatever owns the partitions can SPLIT new ones and
    ///     SWITCH/DROP old ones at runtime without a later schema apply clobbering them.
    ///     <para>
    ///         Reach for this only when something genuinely outside Polecat owns the partitions. For an
    ///         ordinary time-series retention table prefer
    ///         <see cref="ByRollingRange(PartitionPeriod,int,int,TimeProvider)" />, which keeps the whole
    ///         lifecycle inside Weasel's schema model instead of leaving the application to hand-write
    ///         <c>NEXT USED</c>/<c>SPLIT</c>/<c>MERGE</c> DDL on a schedule forever.
    ///     </para>
    /// </summary>
    public DocumentMappingExpression<T> ByExternallyManagedRange(params TValue[] initialBoundaries)
    {
        _parent.SetPartitioning(_member, initialBoundaries, externallyManaged: true);
        return _parent;
    }
}

/// <summary>
///     Schema configuration for document types. Accessed via StoreOptions.Schema.
/// </summary>
public class SchemaConfiguration
{
    internal readonly List<object> Expressions = new();

    /// <summary>
    ///     Configure storage for a document type, including hierarchy registration.
    /// </summary>
    public DocumentMappingExpression<T> For<T>()
    {
        var expr = new DocumentMappingExpression<T>();
        Expressions.Add(expr);
        return expr;
    }
}
