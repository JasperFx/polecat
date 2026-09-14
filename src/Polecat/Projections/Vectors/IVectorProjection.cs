namespace Polecat.Projections.Vectors;

/// <summary>
///     Marks a vector projection so the store can refuse a non-Async registration by name.
/// </summary>
/// <remarks>
///     ⚠️ <b>This exists because a bare <c>IProjection</c> cannot participate in configuration
///     validation at all.</b> <c>ProjectionGraph.AssertValidity</c> walks the registered SOURCES and
///     picks out <c>IValidatedProjection&lt;T&gt;</c>, but <c>Projections.Add(projection, lifecycle)</c>
///     wraps a bare projection in a <c>ProjectionWrapper</c> that does not forward validation — so a
///     projection implementing the interface is never asked. Raised as jasperfx#845; until that is closed,
///     Polecat reads this marker in its own validation pass instead.
/// </remarks>
internal interface IVectorProjection
{
    /// <summary>The projection's type name, for the refusal message.</summary>
    string DescribeSelf();
}
