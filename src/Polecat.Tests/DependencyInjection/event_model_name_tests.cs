using Microsoft.Extensions.DependencyInjection;
using Polecat.Tests.Harness;

namespace Polecat.Tests.DependencyInjection;

public interface IModelNameTestStore: IDocumentStore;

/// <summary>
///     The Event Model name a store contributes its View slices under (fisher#271).
/// </summary>
/// <remarks>
///     <para>
///         Slices merge by model name, so a host that names its own model has to say so here too —
///         otherwise the store contributes under the default and the host assembles TWO models that
///         never meet, surfacing as "expected exactly one assembled model" rather than as anything
///         about Polecat.
///     </para>
/// </remarks>
public class event_model_name_tests
{
    /// <summary>
    ///     A blank name is refused by name, on both entry points, before anything is registered.
    /// </summary>
    /// <remarks>
    ///     An empty string is a legal model name, so it reproduces the very bug the parameter exists
    ///     to prevent, with a blank where the name should be — harder to trace than "EventModel".
    ///     Null stays the documented way to say "the default model", so only a non-null blank is an
    ///     error.
    /// </remarks>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void a_blank_event_model_name_is_refused(string blank)
    {
        Should.Throw<ArgumentException>(() =>
                new ServiceCollection().AddPolecat(
                    (StoreOptions o) => { o.ConnectionString = ConnectionSource.ConnectionString; }, blank))
            .Message.ShouldContain("cannot be empty or whitespace");

        Should.Throw<ArgumentException>(() =>
                new ServiceCollection().AddPolecatStore<IModelNameTestStore>(
                    (StoreOptions o) => { o.ConnectionString = ConnectionSource.ConnectionString; }, blank))
            .Message.ShouldContain("cannot be empty or whitespace");
    }

    /// <summary>
    ///     Null and an ordinary name are both accepted — the refusal is narrow.
    /// </summary>
    [Fact]
    public void a_null_or_ordinary_name_is_accepted()
    {
        Should.NotThrow(() =>
            new ServiceCollection().AddPolecat(
                (StoreOptions o) => { o.ConnectionString = ConnectionSource.ConnectionString; }));

        Should.NotThrow(() =>
            new ServiceCollection().AddPolecat(
                (StoreOptions o) => { o.ConnectionString = ConnectionSource.ConnectionString; }, "Ledgers"));
    }
}
