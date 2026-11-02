using JasperFx.Core.Reflection;
using JasperFx.Events.Upcasting;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record MaskedOld(string Name);

public record MaskedNew(string Name);

public record MaskedLatest(string Name);

public interface IHasMaskedName;

public record MaskedByInterface(string Name) : IHasMaskedName;

public class MaskedOldUpcaster : EventUpcaster<MaskedOld, MaskedNew>
{
    protected override MaskedNew Upcast(MaskedOld oldEvent) => new(oldEvent.Name);
}

/// <summary>
///     #860: an upcast source's rows are read as the upcast's target and masked as what they read as, so a
///     rule naming the source matches nothing. The pass still reports success and the plaintext stays in
///     the row, which is not a failure a feature for removing data can be allowed to have quietly.
/// </summary>
public class masking_rule_on_an_upcast_source_tests
{
    private static void Build(Action<StoreOptions> configure)
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            configure(opts);
        });
    }

    private static string RefusalOf(Action<StoreOptions> configure)
        => Should.Throw<InvalidOperationException>(() => Build(configure)).Message;

    [Fact]
    public void a_rule_on_the_source_is_refused_when_it_comes_after_the_upcast()
    {
        var message = RefusalOf(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedNew>(old => new MaskedNew(old.Name));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(e => e with { Name = "****" });
        });

        message.ShouldContain(typeof(MaskedOld).FullNameInCode());
        message.ShouldContain(typeof(MaskedNew).FullNameInCode());
        message.ShouldContain($"AddMaskingRuleForProtectedInformation<{typeof(MaskedNew).FullNameInCode()}>");
    }

    [Fact]
    public void a_rule_on_the_source_is_refused_when_it_comes_before_the_upcast()
    {
        var message = RefusalOf(opts =>
        {
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(e => e with { Name = "****" });
            opts.Events.Upcast<MaskedOld, MaskedNew>(old => new MaskedNew(old.Name));
        });

        message.ShouldContain(typeof(MaskedNew).FullNameInCode());
    }

    [Fact]
    public void an_action_rule_is_refused_as_well()
    {
        RefusalOf(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedNew>(old => new MaskedNew(old.Name));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(_ => { });
        }).ShouldContain(typeof(MaskedOld).FullNameInCode());
    }

    [Fact]
    public void an_async_upcast_counts()
    {
        RefusalOf(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedNew>((old, _) => Task.FromResult(new MaskedNew(old.Name)));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(e => e with { Name = "****" });
        }).ShouldContain(typeof(MaskedNew).FullNameInCode());
    }

    [Fact]
    public void a_class_based_upcaster_counts()
    {
        RefusalOf(opts =>
        {
            opts.Events.Upcast<MaskedOldUpcaster>();
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(e => e with { Name = "****" });
        }).ShouldContain(typeof(MaskedNew).FullNameInCode());
    }

    [Fact]
    public void an_upcast_claiming_an_explicit_name_counts()
    {
        RefusalOf(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedNew>("some_older_name", old => new MaskedNew(old.Name));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(e => e with { Name = "****" });
        }).ShouldContain(typeof(MaskedNew).FullNameInCode());
    }

    [Fact]
    public void an_async_upcast_claiming_an_explicit_name_counts()
    {
        RefusalOf(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedNew>("some_older_name",
                (old, _) => Task.FromResult(new MaskedNew(old.Name)));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(e => e with { Name = "****" });
        }).ShouldContain(typeof(MaskedNew).FullNameInCode());
    }

    [Fact]
    public void a_rule_on_the_target_is_what_the_refusal_recommends()
    {
        Build(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedNew>(old => new MaskedNew(old.Name));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedNew>(e => e with { Name = "****" });
        });
    }

    /// <summary>The same name on both sides, so a rule there sees every row and is the right one.</summary>
    [Fact]
    public void a_rule_under_a_raw_json_upcast_of_the_same_name_is_fine()
    {
        Build(opts =>
        {
            opts.Events.Upcast<MaskedNew>(json => new MaskedNew(json.RootElement.GetProperty("name").GetString()!));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedNew>(e => e with { Name = "****" });
        });
    }

    /// <summary>The middle of a chain is a source, but older rows do read as it.</summary>
    [Fact]
    public void a_rule_on_a_type_some_rows_read_as_is_fine()
    {
        Build(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedNew>(old => new MaskedNew(old.Name));
            opts.Events.Upcast<MaskedNew, MaskedLatest>(old => new MaskedLatest(old.Name));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskedNew>(e => e with { Name = "****" });
        });
    }

    [Fact]
    public void a_rule_on_an_interface_is_fine()
    {
        Build(opts =>
        {
            opts.Events.Upcast<MaskedOld, MaskedByInterface>(old => new MaskedByInterface(old.Name));
            opts.Events.AddMaskingRuleForProtectedInformation<IHasMaskedName>(_ => { });
        });
    }

    [Fact]
    public void rules_with_no_upcasts_are_fine()
    {
        Build(opts => opts.Events.AddMaskingRuleForProtectedInformation<MaskedOld>(e => e with { Name = "****" }));
    }
}
