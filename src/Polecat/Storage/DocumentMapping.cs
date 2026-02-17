using System.Reflection;

namespace Polecat.Storage;

/// <summary>
///     Discovers and caches metadata about a document type: ID property, table name, ID accessors.
/// </summary>
internal class DocumentMapping
{
    private readonly PropertyInfo _idProperty;
    private readonly Type _documentType;

    public DocumentMapping(Type documentType, StoreOptions options)
    {
        _documentType = documentType;

        _idProperty = FindIdProperty(documentType)
            ?? throw new InvalidOperationException(
                $"Document type '{documentType.FullName}' must have a public property named 'Id' of type Guid or string.");

        IdType = _idProperty.PropertyType;

        if (IdType != typeof(Guid) && IdType != typeof(string))
        {
            throw new InvalidOperationException(
                $"Document type '{documentType.FullName}' has an Id property of type '{IdType.Name}', " +
                "but only Guid and string are supported.");
        }

        var tableName = $"pc_doc_{documentType.Name.ToLowerInvariant()}";
        QualifiedTableName = $"[{options.DatabaseSchemaName}].[{tableName}]";
        TableName = tableName;
        DatabaseSchemaName = options.DatabaseSchemaName;
        DotNetTypeName = $"{documentType.FullName}, {documentType.Assembly.GetName().Name}";
    }

    public Type DocumentType => _documentType;
    public Type IdType { get; }
    public string TableName { get; }
    public string QualifiedTableName { get; }
    public string DatabaseSchemaName { get; }
    public string DotNetTypeName { get; }

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
