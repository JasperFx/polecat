using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx;
using JasperFx.Core.Reflection;
using Polecat.Attributes;
using Polecat.Metadata;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage.Metadata;

namespace Polecat.Storage;

/// <summary>
///     Discovers and caches metadata about a document type: ID property, table name, ID accessors.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: AddSubClassHierarchy uses Assembly.GetTypes() to discover subclasses; document hierarchies are part of the registered surface and AOT consumers must preserve subclass types via their JsonSerializerContext / per-type registration.")]
[UnconditionalSuppressMessage("Trimming", "IL2070:DynamicallyAccessedMembers",
    Justification = "Class-level: reflects PublicProperties on the document Type (FindIdProperty, DiscoverIndexAttributes). The document type is preserved at the registration boundary (Schema.For<T>()), where T flows in from caller code that trimming sees.")]
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
                $"Document type '{documentType.FullName}' must have a public property named 'Id' " +
                "or a property marked with [Identity] of type Guid, string, int, or long.");

        IdType = _idProperty.PropertyType;

        // Unwrap Nullable<T> for strongly-typed ID detection (e.g., PaymentId? -> PaymentId)
        var idTypeToCheck = Nullable.GetUnderlyingType(IdType) ?? IdType;

        if (!SupportedIdTypes.Contains(idTypeToCheck))
        {
            // Check for strongly typed ID wrapper (e.g., record struct OrderId(Guid Value))
            ValueTypeId = TryResolveValueTypeId(idTypeToCheck);
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
        JsonColumnType = options.JsonColumnType;

        // Discover and register attribute-based indexes
        DiscoverIndexAttributes(documentType);

        // #243: discover attribute-based document metadata mappings ([CorrelationIdMetadata], etc.)
        DiscoverMetadataAttributes(documentType);

        // Detect soft delete: [SoftDeleted] attribute, ISoftDeleted interface, or policy
        if (documentType.GetCustomAttribute<SoftDeletedAttribute>() != null
            || typeof(ISoftDeleted).IsAssignableFrom(documentType)
            || options.Policies.IsSoftDeleted(documentType))
        {
            DeleteStyle = DeleteStyle.SoftDelete;
        }

        // Detect optimistic concurrency: IVersioned (Guid), IRevisioned (int), or ILongVersioned (long)
        if (typeof(IVersioned).IsAssignableFrom(documentType))
        {
            UseOptimisticConcurrency = true;
        }
        else if (typeof(IRevisioned).IsAssignableFrom(documentType))
        {
            UseNumericRevisions = true;
        }
        else if (typeof(ILongVersioned).IsAssignableFrom(documentType))
        {
            UseNumericRevisions = true;
            UseLongRevisions = true;
        }
    }

    public Type DocumentType => _documentType;
    public Type IdType { get; }

    /// <summary>
    ///     #223: resolves an index JSON path (e.g. "$.serviceName", "$.address.city") back to the
    ///     CLR member type so a computed-column index can be typed from the member rather than
    ///     defaulting every column to varchar(250). Nullable types are unwrapped. Returns null when
    ///     the path can't be walked to a simple property (caller falls back to varchar(250)).
    /// </summary>
    internal Type? ResolveClrMemberType(string jsonPath)
    {
        if (!jsonPath.StartsWith("$.", StringComparison.Ordinal)) return null;

        var current = _documentType;
        foreach (var segment in jsonPath[2..].Split('.'))
        {
            // Index paths are camelCased property names; match case-insensitively since
            // camelCasing only changes the first character.
            var prop = current.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(p => string.Equals(p.Name, segment, StringComparison.OrdinalIgnoreCase));
            if (prop == null) return null;
            current = prop.PropertyType;
        }

        return Nullable.GetUnderlyingType(current) ?? current;
    }

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
    public string JsonColumnType { get; }
    public DeleteStyle DeleteStyle { get; } = DeleteStyle.Remove;

    /// <summary>
    ///     When true, uses Guid-based optimistic concurrency (IVersioned interface).
    /// </summary>
    public bool UseOptimisticConcurrency { get; }

    /// <summary>
    ///     When true, uses numeric revision tracking (IRevisioned int or ILongVersioned long interface).
    /// </summary>
    public bool UseNumericRevisions { get; }

    /// <summary>
    ///     When true, the numeric revision is tracked as a 64-bit long (ILongVersioned) rather than a
    ///     32-bit int (IRevisioned). Only meaningful when <see cref="UseNumericRevisions" /> is true.
    ///     Recommended for MultiStreamProjection-derived views where Version is the global event sequence.
    /// </summary>
    public bool UseLongRevisions { get; }

    /// <summary>
    ///     Registered subclass types for this document hierarchy.
    /// </summary>
    public List<SubClassMapping> SubClasses { get; } = new();

    /// <summary>
    ///     Custom indexes configured for this document type.
    /// </summary>
    public List<DocumentIndex> Indexes { get; } = new();

    /// <summary>
    ///     #243: document metadata configuration (opt-in columns + member mappings), populated from
    ///     metadata attributes and the <c>Schema.For&lt;T&gt;().Metadata(...)</c> DSL.
    /// </summary>
    public DocumentMetadataConfig Metadata { get; } = new();

    /// <summary>
    ///     Foreign key constraints configured for this document type.
    /// </summary>
    public List<DocumentForeignKey> ForeignKeys { get; } = new();

    /// <summary>
    ///     Declarative SQL Server RANGE partitioning for this document's table, or null when the table
    ///     is not partitioned. Configured via <see cref="DocumentMappingExpression{T}.PartitionByRange" />.
    /// </summary>
    public DocumentPartitioning? Partitioning { get; set; }

    /// <summary>True when a promoted partition column must be written on every upsert.</summary>
    public bool HasPartitionColumn => Partitioning is { RequiresDuplicatedColumn: true };

    /// <summary>SQL fragment appended to an INSERT column list for the partition column (or empty).</summary>
    public string PartitionInsertColumns => HasPartitionColumn ? $", {Partitioning!.ColumnName}" : string.Empty;

    /// <summary>SQL fragment appended to an INSERT VALUES list for the partition column (or empty).</summary>
    public string PartitionInsertValues => HasPartitionColumn ? ", @partition_value" : string.Empty;

    /// <summary>SQL fragment appended to an UPDATE SET clause for the partition column (or empty).</summary>
    public string PartitionUpdateSet =>
        HasPartitionColumn ? $", {Partitioning!.ColumnName} = @partition_value" : string.Empty;

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
            // If id is already the wrapper type (e.g., PaymentId), use it directly
            var idActualType = id.GetType();
            var wrapperType = Nullable.GetUnderlyingType(IdType) ?? IdType;
            if (idActualType == wrapperType)
            {
                _idProperty.SetValue(document, id);
                return;
            }

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
        // Polecat#135 (jasperfx#335): delegate to the lifted, side-effect-free
        // JasperFx.DocumentIdentity.FindIdMember. We pass Polecat's own valid-id-type
        // predicate (canonical scalars + strong-typed-id wrappers via TryResolveValueTypeId)
        // so strong-typed-id "Id" members are recognized as candidates — the shared helper's
        // default ValidIdTypes set is scalar-only and would skip them. The result is filtered
        // to PropertyInfo to preserve Polecat's property-only contract (both call sites need
        // PropertyInfo for .PropertyType / .SetValue); the shared helper also finds fields,
        // which Polecat does not use.
        return DocumentIdentity.FindIdMember(type, IsValidIdType) as PropertyInfo;
    }

    private static bool IsValidIdType(Type candidate)
    {
        var underlying = Nullable.GetUnderlyingType(candidate) ?? candidate;
        return SupportedIdTypes.Contains(underlying) || TryResolveValueTypeId(underlying) != null;
    }

    /// <summary>
    ///     #243: scans the document type for metadata attributes ([CorrelationIdMetadata], etc.) and
    ///     enables + maps the corresponding metadata columns.
    /// </summary>
    private void DiscoverMetadataAttributes(Type documentType)
    {
        var members = documentType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Cast<MemberInfo>()
            .Concat(documentType.GetFields(BindingFlags.Public | BindingFlags.Instance));

        foreach (var member in members)
        {
            foreach (var attr in member.GetCustomAttributes<MetadataAttribute>())
            {
                attr.Apply(Metadata, member);
            }
        }
    }

    /// <summary>
    ///     Scans the document type for [Index] and [UniqueIndex] attribute-based indexes
    ///     and auto-registers them.
    /// </summary>
    private void DiscoverIndexAttributes(Type documentType)
    {
        var properties = documentType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        // Discover [Index] attributes — each property gets its own index
        foreach (var prop in properties)
        {
            var indexAttr = prop.GetCustomAttribute<IndexAttribute>();
            if (indexAttr == null) continue;

            var jsonPath = DocumentIndex.MemberToJsonPath(prop);
            var index = new DocumentIndex([jsonPath])
            {
                IndexName = indexAttr.IndexName,
                SortOrder = indexAttr.SortOrder,
                Casing = indexAttr.Casing
            };
            if (indexAttr.SqlType != null) index.SqlType = indexAttr.SqlType;
            Indexes.Add(index);
        }

        // Discover [UniqueIndex] attributes — group by IndexName for composite unique indexes
        var uniqueProps = properties
            .Select(p => new { Property = p, Attr = p.GetCustomAttribute<UniqueIndexAttribute>() })
            .Where(x => x.Attr != null)
            .ToList();

        // Group by IndexName: properties with the same IndexName form a composite unique index
        var groups = uniqueProps.GroupBy(x => x.Attr!.IndexName ?? x.Property.Name);
        foreach (var group in groups)
        {
            var members = group.ToList();
            var firstAttr = members[0].Attr!;
            var jsonPaths = members.Select(m => DocumentIndex.MemberToJsonPath(m.Property)).ToArray();

            var index = new DocumentIndex(jsonPaths)
            {
                IsUnique = true,
                IndexName = firstAttr.IndexName,
                TenancyScope = firstAttr.TenancyScope,
                Casing = firstAttr.Casing
            };
            if (firstAttr.SqlType != null) index.SqlType = firstAttr.SqlType;
            Indexes.Add(index);
        }
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
