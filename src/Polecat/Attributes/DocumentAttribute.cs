namespace Polecat.Attributes;

/// <summary>
///     Marker attribute for source generator discovery. When applied to a class,
///     the Polecat source generator emits a typed document provider with pre-computed SQL.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class DocumentAttribute : Attribute
{
    /// <summary>
    ///     Optionally specify the Id property type. If not set, the generator
    ///     discovers it by convention (public Id property of type Guid or string).
    /// </summary>
    public Type? IdType { get; set; }
}
