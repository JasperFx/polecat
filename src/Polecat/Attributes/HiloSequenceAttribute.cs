namespace Polecat.Attributes;

/// <summary>
///     Use to customize the HiLo sequence generation for a single document type.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class HiloSequenceAttribute : Attribute
{
    public int MaxLo { get; set; }
    public string? SequenceName { get; set; }
}
