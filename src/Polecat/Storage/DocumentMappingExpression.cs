namespace Polecat.Storage;

/// <summary>
///     Fluent configuration builder for a document type's mapping.
///     Used via StoreOptions.Schema.For&lt;T&gt;().
/// </summary>
public class DocumentMappingExpression<T>
{
    internal readonly Type DocumentType = typeof(T);
    internal readonly List<(Type SubClass, string? Alias)> SubClasses = new();

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
