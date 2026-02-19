using System.Reflection;
using Polecat.Attributes;
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
            throw new InvalidOperationException(
                $"Document type '{documentType.FullName}' has an Id property of type '{IdType.Name}', " +
                "but only Guid, string, int, and long are supported.");
        }

        IsNumericId = IdType == typeof(int) || IdType == typeof(long);

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
    }

    public Type DocumentType => _documentType;
    public Type IdType { get; }
    public bool IsNumericId { get; }
    public HiloSettings? HiloSettings { get; set; }
    public string TableName { get; }
    public string QualifiedTableName { get; }
    public string DatabaseSchemaName { get; }
    public string DotNetTypeName { get; }
    public TenancyStyle TenancyStyle { get; }

    public object GetId(object document)
    {
        return _idProperty.GetValue(document)
            ?? throw new InvalidOperationException(
                $"Document of type '{_documentType.Name}' has a null Id.");
    }

    public void SetId(object document, object id)
    {
        _idProperty.SetValue(document, id);
    }

    public static PropertyInfo? FindIdProperty(Type type)
    {
        return type.GetProperty("Id", BindingFlags.Public | BindingFlags.Instance);
    }
}
