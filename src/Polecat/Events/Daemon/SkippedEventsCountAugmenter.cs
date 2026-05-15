using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;

namespace Polecat.Events.Daemon;

/// <summary>
///     Subscribes to <see cref="ShardStateTracker"/> and populates
///     <see cref="ShardState.SkippedEventsCount"/> on Skipped publications.
///     The framework's <c>HighWaterAgent</c> owns the publish path via
///     <see cref="ShardStateTracker.MarkSkippingAsync"/>, which sets
///     <see cref="ShardState.PreviousGoodMark"/> but does not compute the
///     count — implementations are expected to populate it themselves.
///     Polecat picks the "most-recent" semantic: the gap between the
///     newly-marked HighWaterMark and the previous good mark.
/// </summary>
internal sealed class SkippedEventsCountAugmenter : IObserver<ShardState>
{
    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(ShardState value)
    {
        if (value.Action == ShardAction.Skipped && value.SkippedEventsCount is null)
        {
            value.SkippedEventsCount = value.Sequence - value.PreviousGoodMark;
        }
    }
}
