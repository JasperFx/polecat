namespace Polecat.Attributes;

/// <summary>
///     Marks a document type for soft deletion. When applied, Delete() will
///     set is_deleted = 1 instead of physically removing the row.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class SoftDeletedAttribute : Attribute;
