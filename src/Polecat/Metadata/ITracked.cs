namespace Polecat.Metadata;

/// <summary>
///     Implement this interface on a document type to automatically have
///     CorrelationId, CausationId, and LastModifiedBy copied from the
///     session to the document on save.
/// </summary>
public interface ITracked
{
    string? CorrelationId { get; set; }
    string? CausationId { get; set; }
    string? LastModifiedBy { get; set; }
}
