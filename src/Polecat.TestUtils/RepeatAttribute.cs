using System.Reflection;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Polecat.TestUtils;

/// <summary>
///     Runs a [Theory] <paramref name="count" /> times, passing the 1-based iteration number.
///     Used by the concurrency suites (HiLo, async daemon) where a single pass proves nothing.
/// </summary>
/// <remarks>
///     xUnit v3 moved <c>DataAttribute</c> from <c>Xunit.Sdk</c> to <c>Xunit.v3</c> and reshaped both
///     of its abstract members: <c>GetData</c> now returns theory data rows asynchronously and takes
///     a <see cref="DisposalTracker" />, and <c>SupportsDiscoveryEnumeration</c> is required. The
///     data here is constant and cheap, so it enumerates at discovery time — which is what keeps
///     each iteration a test case of its own rather than collapsing them into a single one.
/// </remarks>
public class RepeatAttribute(int count) : DataAttribute
{
    public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(
        MethodInfo testMethod,
        DisposalTracker disposalTracker)
    {
        return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(
            Enumerable.Range(1, count).Select(i => (ITheoryDataRow)new TheoryDataRow(i)).ToArray());
    }

    public override bool SupportsDiscoveryEnumeration() => true;
}
