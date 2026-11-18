using Polecat.Internal;
using Polecat.Schema.Identity.Sequences;
using Polecat.Tests.Harness;
using Weasel.Core.Sequences;

namespace Polecat.Tests.HiLo;

/// <summary>
///     #1087 (marten#6058, weasel#849): <c>SetFloor</c> moves the hilo row and installs a block it reserved above
///     it. An advance that reserves after the row moved, and installs first, holds a higher block that is above the
///     floor too. SetFloor's lower block used to replace it, so the ids issued next stepped back behind ones
///     already handed out: above the floor and unique, but out of order.
/// </summary>
public class hilo_set_floor_never_steps_back_tests : OneOffConfigurationsContext
{
    private const long Floor = 1000;

    private FloorHeldSequence NewSequence(long? floor) =>
        new(new ConnectionFactory(ConnectionSource.ConnectionString), theStore.Options,
            "never_steps_back_" + Guid.NewGuid().ToString("N"), 2, floor);

    [Fact]
    public async Task an_advance_that_installs_a_higher_block_first_is_not_replaced_by_the_floors_block()
    {
        var sequence = NewSequence(Floor);

        // Every block outside SetFloor comes from the synchronous advance
        sequence.NextLong().ShouldBe(1);
        sequence.NextLong().ShouldBe(2);

        // SetFloor reserves the block above the floor, then is held before installing it
        var setFloor = sequence.SetFloor(Floor);
        var floorsBlock = await sequence.FloorBlockReserved.WaitAsync(TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken);
        floorsBlock.ShouldBe(Floor / 2 + 1);

        // Meanwhile a caller uses up the block SetFloor installed before it moved the row, and advances past
        // the floor's block in the database
        var issued = new[] { sequence.NextLong(), sequence.NextLong(), sequence.NextLong() };
        var highest = issued.Max();
        highest.ShouldBeGreaterThan(Floor);

        sequence.ReleaseFloor();
        await setFloor.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        // Before the fix the next id was 1003, out of the floor's block, behind the 1005 already issued
        var later = Enumerable.Range(0, 6).Select(_ => sequence.NextLong()).ToArray();
        later.ShouldAllBe(x => x > highest);
        later.ShouldBe(later.Distinct().OrderBy(x => x));
    }

    [Fact]
    public async Task without_another_install_the_floors_block_is_installed_as_given()
    {
        var sequence = NewSequence(floor: null);

        sequence.NextLong().ShouldBe(1);
        await sequence.SetFloor(Floor);

        sequence.NextLong().ShouldBe(Floor + 3);
    }

    /// <summary>
    ///     Holds SetFloor's fetch of the block above the floor between reserving it and returning it.
    /// </summary>
    private sealed class FloorHeldSequence : HiloSequence
    {
        private static readonly AsyncLocal<bool> _insideSetFloor = new();

        private readonly long? _holdAbovePages;
        private readonly TaskCompletionSource<long> _reserved =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _held;

        public FloorHeldSequence(ConnectionFactory connectionFactory, StoreOptions options, string entityName,
            int maxLo, long? floor)
            : base(connectionFactory, options, entityName, new HiloSettings { MaxLo = maxLo })
        {
            _holdAbovePages = floor / maxLo;
        }

        public Task<long> FloorBlockReserved => _reserved.Task;

        public void ReleaseFloor() => _release.TrySetResult();

        public override async Task SetFloor(long floor)
        {
            _insideSetFloor.Value = true;
            try
            {
                await base.SetFloor(floor);
            }
            finally
            {
                _insideSetFloor.Value = false;
            }
        }

        internal override async Task<long> FetchNextHiAsync(CancellationToken ct)
        {
            var hi = await base.FetchNextHiAsync(ct);

            if (_insideSetFloor.Value && _holdAbovePages is { } pages && hi > pages &&
                Interlocked.Exchange(ref _held, 1) == 0)
            {
                _reserved.TrySetResult(hi);
                await _release.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
            }

            return hi;
        }
    }
}
