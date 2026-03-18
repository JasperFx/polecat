using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Storage;

/// <summary>
///     Defines a foreign key relationship from a document property to another document type's table.
///     The FK column is implemented as a persisted computed column (using JSON_VALUE).
/// </summary>
public class DocumentForeignKey
{
    public DocumentForeignKey(string jsonPath, Type referenceDocumentType)
    {
        JsonPath = jsonPath;
        ReferenceDocumentType = referenceDocumentType;
    }

    /// <summary>
    ///     The JSON path of the foreign key property (e.g., "$.assigneeId").
    /// </summary>
    public string JsonPath { get; }

    /// <summary>
    ///     The referenced document type (its table's id column is the target).
    /// </summary>
    public Type ReferenceDocumentType { get; }

    /// <summary>
    ///     Optional explicit constraint name. Auto-generated if null.
    /// </summary>
    public string? ConstraintName { get; set; }

    /// <summary>
    ///     Cascade action for DELETE operations. Default is NoAction.
    /// </summary>
    public CascadeAction OnDelete { get; set; } = CascadeAction.NoAction;

    /// <summary>
    ///     Generates DDL to add the computed column and create the foreign key constraint.
    ///     Returns individual statements to be executed separately.
    /// </summary>
    internal string[] ToDdlStatements(DocumentMapping parentMapping, DocumentMapping referenceMapping)
    {
        var schema = parentMapping.DatabaseSchemaName;
        var table = parentMapping.TableName;
        var qualifiedTable = $"[{schema}].[{table}]";
        var colName = DocumentIndex.ColumnNameForPath(JsonPath);
        var refTable = $"[{referenceMapping.DatabaseSchemaName}].[{referenceMapping.TableName}]";
        var constraintName = ConstraintName ?? DeriveConstraintName(table, colName);

        // Determine SQL type from the reference document's ID type
        var innerIdType = referenceMapping.InnerIdType;
        var sqlType = innerIdType == typeof(Guid) ? "uniqueidentifier"
            : innerIdType == typeof(int) ? "int"
            : innerIdType == typeof(long) ? "bigint"
            : "varchar(250)";

        var statements = new List<string>();

        // Add persisted computed column for the FK property
        statements.Add($"""
            IF COL_LENGTH('{schema}.{table}', '{colName}') IS NULL
                ALTER TABLE {qualifiedTable} ADD [{colName}] AS CAST(JSON_VALUE(data, '{JsonPath}') AS {sqlType}) PERSISTED;
            """);

        // Build ON DELETE clause
        var onDeleteClause = OnDelete switch
        {
            CascadeAction.Cascade => " ON DELETE CASCADE",
            CascadeAction.SetNull => " ON DELETE SET NULL",
            CascadeAction.SetDefault => " ON DELETE SET DEFAULT",
            _ => ""
        };

        // Add FK constraint (conjoined tenancy includes tenant_id)
        var isConjoined = parentMapping.TenancyStyle == TenancyStyle.Conjoined
                       && referenceMapping.TenancyStyle == TenancyStyle.Conjoined;

        var fkCheck = $"NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = '{constraintName}' AND parent_object_id = OBJECT_ID('{qualifiedTable}'))";

        if (isConjoined)
        {
            statements.Add($"""
                IF {fkCheck}
                    ALTER TABLE {qualifiedTable}
                    ADD CONSTRAINT [{constraintName}]
                    FOREIGN KEY (tenant_id, [{colName}]) REFERENCES {refTable} (tenant_id, id){onDeleteClause};
                """);
        }
        else
        {
            statements.Add($"""
                IF {fkCheck}
                    ALTER TABLE {qualifiedTable}
                    ADD CONSTRAINT [{constraintName}]
                    FOREIGN KEY ([{colName}]) REFERENCES {refTable} (id){onDeleteClause};
                """);
        }

        return statements.ToArray();
    }

    private static string DeriveConstraintName(string tableName, string columnName)
    {
        return $"fk_{tableName}_{columnName}";
    }

    /// <summary>
    ///     Resolves a lambda expression to a JSON path for the foreign key property.
    /// </summary>
    internal static string ResolveJsonPath<T>(Expression<Func<T, object?>> expression)
    {
        var body = expression.Body;

        // Unwrap Convert for value types
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression memberExpr)
        {
            return DocumentIndex.MemberToJsonPath(memberExpr.Member);
        }

        throw new ArgumentException(
            $"Expression '{expression}' is not a supported foreign key expression. " +
            "Use a single property (x => x.Prop).");
    }
}

/// <summary>
///     Cascade action for foreign key constraints.
/// </summary>
public enum CascadeAction
{
    NoAction,
    Cascade,
    SetNull,
    SetDefault
}
