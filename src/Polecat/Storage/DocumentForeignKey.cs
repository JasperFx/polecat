using System.Linq.Expressions;
using Weasel.Core;

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
