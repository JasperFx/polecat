using Microsoft.Extensions.DependencyInjection;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.DependencyInjection;

public interface IModelNameTestStore: IDocumentStore;

/// <summary>
///     The Event Model name a store contributes its View slices under (fisher#271, gh-615), set on
///     <see cref="StoreOptions.EventModelName" /> (gh-618).
/// </summary>
/// <remarks>
///     <para>
///         Slices merge by model name, so a host that names its own model has to say so here too —
///         otherwise the store contributes under the default and the host assembles TWO models that
///         never meet, surfacing as "expected exactly one assembled model" rather than as anything
///         about Polecat.
///     </para>
///     <para>
///         <b>On the options rather than as an <c>AddPolecat</c> parameter (gh-618).</b> It is a
///         property of the store, it reaches every configuration path including
///         <c>IConfigurePolecat</c>, and it keeps the three Critter Stack stores configured the same
///         way. The refusal therefore lives on the setter, which is the one place every path goes
///         through.
///     </para>
/// </remarks>
public class event_model_name_tests
{
    /// <summary>
    ///     A blank name is refused, wherever it is set from.
    /// </summary>
    /// <remarks>
    ///     An empty string is a legal model name, so it reproduces the very bug this setting exists to
    ///     prevent, with a blank where the name should be — harder to trace than "EventModel". Null
    ///     stays the documented way to say "the default model", so only a non-null blank is an error.
    /// </remarks>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void a_blank_event_model_name_is_refused(string blank)
    {
        Should.Throw<ArgumentException>(() => new StoreOptions { EventModelName = blank })
            .Message.ShouldContain("cannot be empty or whitespace");

        // And through the registration paths, which is where a user actually writes it.
        Should.Throw<ArgumentException>(() =>
            new ServiceCollection().AddPolecat((StoreOptions o) =>
            {
                o.ConnectionString = ConnectionSource.ConnectionString;
                o.EventModelName = blank;
            }));

        // The ancillary store refuses too, but LATER — and the difference is not an oversight.
        // AddPolecat builds its StoreOptions eagerly so the conditional tenancy registration can
        // probe it (#377); AddPolecatStore<T> defers the lambda until the store is resolved. So the
        // same mistake surfaces at registration for the primary and at first resolution for an
        // ancillary store.
        var services = new ServiceCollection();
        services.AddPolecatStore<IModelNameTestStore>((StoreOptions o) =>
        {
            o.ConnectionString = ConnectionSource.ConnectionString;
            o.EventModelName = blank;
        });

        using var provider = services.BuildServiceProvider();
        Should.Throw<ArgumentException>(() => provider.GetRequiredService<IModelNameTestStore>())
            .Message.ShouldContain("cannot be empty or whitespace");
    }

    /// <summary>
    ///     Null and an ordinary name are both accepted — the refusal is narrow.
    /// </summary>
    [Fact]
    public void a_null_or_ordinary_name_is_accepted()
    {
        new StoreOptions { EventModelName = null }.EventModelName.ShouldBeNull();
        new StoreOptions { EventModelName = "Ledgers" }.EventModelName.ShouldBe("Ledgers");

        Should.NotThrow(() =>
            new ServiceCollection().AddPolecat(
                (StoreOptions o) => { o.ConnectionString = ConnectionSource.ConnectionString; }));

        Should.NotThrow(() =>
            new ServiceCollection().AddPolecat((StoreOptions o) =>
            {
                o.ConnectionString = ConnectionSource.ConnectionString;
                o.EventModelName = "Ledgers";
            }));
    }
}
