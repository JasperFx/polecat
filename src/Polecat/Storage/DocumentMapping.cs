using System.Reflection;
using JasperFx;
using JasperFx.Core.Reflection;
using Polecat.Attributes;
using Polecat.Metadata;
using Polecat.Schema.Identity.Sequences;

namespace Polecat.Storage;

/// <summary>
///     Discovers and caches metadata about a document type: ID property, table name, ID accessors.
/// </summary>
internal class DocumentMapping
{
    private static readonly HashSet<Type> SupportedIdTypes =
        [typeof(Guid), typeof(string), typeof(int), typeof(long)];

    private readonly PropertyInfo _idProperty;
    private readonly Type _documentType;

    public DocumentMapping(Type documentType, StoreOptions options)
    {
        _documentType = documentType;

        _idProperty = FindIdProperty(documentType)
            ?? throw new InvalidOperationException(
                $"Document type '{documentType.FullName}' must have a public property named 'Id' of type Guid, string, int, or long.");

        IdType = _idProperty.PropertyType;

        if (!SupportedIdTypes.Contains(IdType))
        {
            // Check for strongly typed ID wrapper (e.g., record struct OrderId(Guid Value))
            ValueTypeId = TryResolveValueTypeId(IdType);
            if (ValueTypeId == null)
            {
                throw new InvalidOperationException(
                    $"Document type '{documentType.FullName}' has an Id property of type '{IdType.Name}', " +
                    "but only Guid, string, int, long, and value type wrappers around those types are supported.");
            }
        }

        IsNumericId = InnerIdType == typeof(int) || InnerIdType == typeof(long);

        // Read HiloSequenceAttribute if present on numeric ID types
        if (IsNumericId)
        {
            var attr = documentType.GetCustomAttribute<HiloSequenceAttribute>();
            if (attr != null)
            {
                HiloSettings = new HiloSettings();
                if (attr.MaxLo > 0) HiloSettings.MaxLo = attr.MaxLo;
                if (attr.SequenceName != null) HiloSettings.SequenceName = attr.SequenceName;
            }
        }

        var tableName = $"pc_doc_{documentType.Name.ToLowerInvariant()}";
        QualifiedTableName = $"[{options.DatabaseSchemaName}].[{tableName}]";
        TableName = tableName;
        DatabaseSchemaName = options.DatabaseSchemaName;
        DotNetTypeName = $"{documentType.FullName}, {documentType.Assembly.GetName().Name}";
        TenancyStyle = options.Events.TenancyStyle;

        // Detect soft delete: [SoftDeleted] attribute, ISoftDeleted interface, or policy
        if (documentType.GetCustomAttribute<SoftDeletedAttribute>() != null
            || typeof(ISoftDeleted).IsAssignableFrom(documentType)
            || options.Policies.IsSoftDeleted(documentType))
        {
            DeleteStyle = DeleteStyle.SoftDelete;
        }

        // Detect optimistic concurrency: IVersioned (Guid) or IRevisioned (int)
        if (typeof(IVersioned).IsAssignableFrom(documentType))
        {
            UseOptimisticConcurrency = true;
        }
        else if (typeof(IRevisioned).IsAssignableFrom(documentType))
        {
            UseNumericRevisions = true;
        }
    }

    public Type DocumentType => _documentType;
    public Type IdType { get; }

    /// <summary>
    ///     The unwrapped inner type for SQL column mapping.
    ///     For strongly typed IDs (e.g., OrderId wrapping Guid), returns the inner type.
    ///     For plain IDs, returns IdType.
    /// </summary>
    public Type InnerIdType => ValueTypeId?.SimpleType ?? IdType;

    /// <summary>
    ///     Non-null when the Id property is a strongly typed value wrapper.
    /// </summary>
    public ValueTypeInfo? ValueTypeId { get; }

    /// <summary>
    ///     True when the Id property uses a strongly typed wrapper.
    /// </summary>
    public bool IsStrongTypedId => ValueTypeId != null;

    public bool IsNumericId { get; }
    public HiloSettings? HiloSettings { get; set; }
    public string TableName { get; }
    public string QualifiedTableName { get; }
    public string DatabaseSchemaName { get; }
    public string DotNetTypeName { get; }
    public TenancyStyle TenancyStyle { get; }
    public DeleteStyle DeleteStyle { get; } = DeleteStyle.Remove;

    /// <summary>
    ///     When true, uses Guid-based optimistic concurrency (IVersioned interface).
    /// </summary>
    public bool UseOptimisticConcurrency { get; }

    /// <summary>
    ///     When true, uses int-based numeric revision tracking (IRevisioned interface).
    /// </summary>
    public bool UseNumericRevisions { get; }

    /// <summary>
    ///     Registered subclass types for this document hierarchy.
    /// </summary>
    public List<SubClassMapping> SubClasses { get; } = new();

    /// <summary>
    ///     Custom indexes configured for this document type.
    /// </summary>
    public List<DocumentIndex> Indexes { get; } = new();

    /// <summary>
    ///     Foreign key constraints configured for this document type.
    /// </summary>
    public List<DocumentForeignKey> ForeignKeys { get; } = new();

    /// <summary>
    ///     The alias used in the doc_type discriminator column for the base type.
    /// </summary>
    public string Alias { get; set; } = "base";

    /// <summary>
    ///     True when this mapping has subclasses registered, or the document type is abstract/interface.
    /// </summary>
    public bool IsHierarchy() =>
        SubClasses.Count > 0
        || _documentType.IsAbstract
        || _documentType.IsInterface;

    /// <summary>
    ///     Get the doc_type alias for a given runtime type.
    /// </summary>
    public string AliasFor(Type subclassType)
    {
        if (subclassType == _documentType) return Alias;
        var sub = SubClasses.FirstOrDefault(x => x.DocumentType == subclassType);
        if (sub == null)
            throw new ArgumentOutOfRangeException(nameof(subclassType),
                $"Type '{subclassType.Name}' is not a registered subclass of '{_documentType.Name}'.");
        return sub.Alias;
    }

    /// <summary>
    ///     Resolve a doc_type alias back to its .NET type.
    /// </summary>
    public Type TypeFor(string alias)
    {
        if (string.Equals(alias, Alias, StringComparison.OrdinalIgnoreCase)) return _documentType;
        var sub = SubClasses.FirstOrDefault(x =>
            string.Equals(x.Alias, alias, StringComparison.OrdinalIgnoreCase));
        if (sub == null)
            throw new ArgumentOutOfRangeException(nameof(alias),
                $"Unknown doc_type alias '{alias}' for document type '{_documentType.Name}'.");
        return sub.DocumentType;
    }

    /// <summary>
    ///     Register a subclass type.
    /// </summary>
    public void AddSubClass(Type subclassType, string? alias = null)
    {
        if (!_documentType.IsAssignableFrom(subclassType))
            throw new ArgumentException(
                $"Type '{subclassType.Name}' does not inherit from '{_documentType.Name}'.");

        if (SubClasses.Any(x => x.DocumentType == subclassType)) return;
        SubClasses.Add(new SubClassMapping(subclassType, alias));
    }

    /// <summary>
    ///     Auto-discover and register all subclasses from the base type's assembly.
    /// </summary>
    public void AddSubClassHierarchy()
    {
        var assembly = _documentType.Assembly;
        foreach (var type in assembly.GetTypes())
        {
            if (type != _documentType && _documentType.IsAssignableFrom(type) && !type.IsAbstract)
            {
                AddSubClass(type);
            }
        }
    }

    /// <summary>
    ///     Returns the unwrapped inner ID value for SQL parameters.
    ///     For strongly typed IDs, extracts the inner value (e.g., OrderId → Guid).
    /// </summary>
    public object GetId(object document)
    {
        var raw = _idProperty.GetValue(document)
            ?? throw new InvalidOperationException(
                $"Document of type '{_documentType.Name}' has a null Id.");

        if (ValueTypeId != null)
        {
            return ValueTypeId.ValueProperty.GetValue(raw)!;
        }

        return raw;
    }

    /// <summary>
    ///     Returns the raw (possibly wrapped) ID value from the document.
    ///     Used for identity map keys where wrapper type matters.
    /// </summary>
    public object GetRawId(object document)
    {
        return _idProperty.GetValue(document)
            ?? throw new InvalidOperationException(
                $"Document of type '{_documentType.Name}' has a null Id.");
    }

    /// <summary>
    ///     Sets the ID on a document. For strongly typed IDs, wraps the inner value first.
    /// </summary>
    public void SetId(object document, object id)
    {
        if (ValueTypeId != null)
        {
            // Wrap: Guid → OrderId
            object wrapped;
            if (ValueTypeId.Ctor != null)
            {
                wrapped = ValueTypeId.Ctor.Invoke([id]);
            }
            else
            {
                wrapped = ValueTypeId.Builder!.Invoke(null, [id])!;
            }

            _idProperty.SetValue(document, wrapped);
        }
        else
        {
            _idProperty.SetValue(document, id);
        }
    }

    public static PropertyInfo? FindIdProperty(Type type)
    {
        return type.GetProperty("Id", BindingFlags.Public | BindingFlags.Instance);
    }

    /// <summary>
    ///     Attempts to resolve a value type as a strongly typed ID wrapper.
    ///     Returns null if the type isn't a valid wrapper around a supported ID type.
    /// </summary>
    private static ValueTypeInfo? TryResolveValueTypeId(Type idType)
    {
        if (!idType.IsValueType || idType.IsPrimitive || idType.IsEnum) return null;

        try
        {
            var info = ValueTypeInfo.ForType(idType);
            if (SupportedIdTypes.Contains(info.SimpleType))
            {
                return info;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }
}
