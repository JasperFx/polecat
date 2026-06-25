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
    internal readonly List<JsonIndex> JsonIndexes = new();
    internal readonly List<DocumentForeignKey> ForeignKeys = new();
    internal DocumentPartitioning? Partitioning;

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
    ///     Properties are extracted via JSON_VALUE from the data column. Pass <paramref name="include" />
    ///     (a member or anonymous type) to carry those members as non-key INCLUDE columns for a
    ///     covering index that avoids key lookups.
    /// </summary>
    public DocumentMappingExpression<T> Index(Expression<Func<T, object?>> expression,
        Action<DocumentIndex>? configure = null, Expression<Func<T, object?>>? include = null)
    {
        var paths = DocumentIndex.ResolveJsonPaths(expression);
        var index = new DocumentIndex(paths);
        if (include != null) index.IncludePaths = DocumentIndex.ResolveJsonPaths(include);
        configure?.Invoke(index);
        Indexes.Add(index);
        return this;
    }

    /// <summary>
    ///     Add a unique index on one or more document properties. Pass <paramref name="include" /> to
    ///     carry extra members as non-key INCLUDE columns (covering index).
    /// </summary>
    public DocumentMappingExpression<T> UniqueIndex(Expression<Func<T, object?>> expression,
        Action<DocumentIndex>? configure = null, Expression<Func<T, object?>>? include = null)
    {
        var paths = DocumentIndex.ResolveJsonPaths(expression);
        var index = new DocumentIndex(paths) { IsUnique = true };
        if (include != null) index.IncludePaths = DocumentIndex.ResolveJsonPaths(include);
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
    ///     Add a native SQL Server 2025 JSON index (<c>CREATE JSON INDEX</c>) over one or more JSON
    ///     paths in the document. A single JSON index covers all the given paths and accelerates
    ///     JSON_VALUE (=) / JSON_PATH_EXISTS / JSON_CONTAINS predicates. Requires
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
    ///     Add a native JSON index over the entire JSON document (no <c>FOR</c> path filter).
    /// </summary>
    public DocumentMappingExpression<T> JsonIndex(Action<JsonIndex>? configure = null)
    {
        var index = new JsonIndex([]);
        configure?.Invoke(index);
        JsonIndexes.Add(index);
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
