using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

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
    ///     Add a computed index on one or more document properties.
    ///     Properties are extracted via JSON_VALUE from the data column.
    /// </summary>
    public DocumentMappingExpression<T> Index(Expression<Func<T, object?>> expression,
        Action<DocumentIndex>? configure = null)
    {
        var paths = DocumentIndex.ResolveJsonPaths(expression);
        var index = new DocumentIndex(paths);
        configure?.Invoke(index);
        Indexes.Add(index);
        return this;
    }

    /// <summary>
    ///     Add a unique index on one or more document properties.
    /// </summary>
    public DocumentMappingExpression<T> UniqueIndex(Expression<Func<T, object?>> expression,
        Action<DocumentIndex>? configure = null)
    {
        var paths = DocumentIndex.ResolveJsonPaths(expression);
        var index = new DocumentIndex(paths) { IsUnique = true };
        configure?.Invoke(index);
        Indexes.Add(index);
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
    ///     <c>PartitionOn(x =&gt; x.Member)</c>. Follow with <see cref="PartitioningExpression{T,TValue}.ByRange" />
    ///     (Polecat manages + rolls the boundaries) or
    ///     <see cref="PartitioningExpression{T,TValue}.ByExternallyManagedRange" /> (Polecat provisions the
    ///     partitioned table once, then leaves the partitions to be managed externally — the
    ///     time-series-retention pattern).
    /// </summary>
    public PartitioningExpression<T, TValue> PartitionOn<TValue>(Expression<Func<T, TValue>> member)
    {
        return new PartitioningExpression<T, TValue>(this, member);
    }

    /// <summary>Internal hook used by <see cref="PartitioningExpression{T,TValue}" /> to set the descriptor.</summary>
    internal void SetPartitioning<TValue>(Expression<Func<T, TValue>> member, TValue[] boundaries,
        bool externallyManaged)
    {
        var idMemberName = DocumentMapping.FindIdProperty(typeof(T))?.Name ?? "Id";
        Partitioning = DocumentPartitioning.For(member, boundaries, idMemberName, externallyManaged);
    }
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
    ///     #255: externally-managed RANGE partitioning. Polecat creates the partition function/scheme
    ///     and table once (with the supplied <paramref name="initialBoundaries" />) and then never
    ///     reconciles the partitioning, so the app/DBA can SPLIT new partitions and SWITCH/DROP old
    ///     ones at runtime for time-series retention without a later schema apply clobbering them.
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
