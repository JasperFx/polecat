using Polecat.Internal;
using Shouldly;

namespace Polecat.Tests.Storage;

/// <summary>
///     Unit coverage for the shared escaping helper introduced by the polecat#390 audit. These are the
///     two positions T-SQL requires escaping in, and the two mistakes the audit was looking for.
/// </summary>
public class sql_escaping_tests
{
    [Fact]
    public void quote_identifier_doubles_an_embedded_closing_bracket()
    {
        // Without the doubling the bracket closes early and everything after it is parsed as SQL.
        SqlEscaping.QuoteIdentifier("plain").ShouldBe("[plain]");
        SqlEscaping.QuoteIdentifier("we]ird").ShouldBe("[we]]ird]");
        SqlEscaping.QuoteIdentifier("a]; DROP TABLE x --").ShouldBe("[a]]; DROP TABLE x --]");
    }

    [Fact]
    public void quote_identifier_leaves_quotes_and_dots_alone()
    {
        // A single quote is not special inside a bracketed identifier, and a dot must NOT be treated
        // as a separator — [a.b] is one object named "a.b".
        SqlEscaping.QuoteIdentifier("o'brien").ShouldBe("[o'brien]");
        SqlEscaping.QuoteIdentifier("a.b").ShouldBe("[a.b]");
    }

    [Fact]
    public void qualified_name_escapes_both_halves_independently()
    {
        SqlEscaping.QualifiedName("dbo", "pc_events").ShouldBe("[dbo].[pc_events]");
        SqlEscaping.QualifiedName("sch]ema", "tab]le").ShouldBe("[sch]]ema].[tab]]le]");
    }

    [Fact]
    public void literal_doubles_an_embedded_single_quote()
    {
        SqlEscaping.Literal("plain").ShouldBe("'plain'");
        SqlEscaping.Literal("o'brien").ShouldBe("'o''brien'");
        SqlEscaping.LiteralBody("o'brien").ShouldBe("o''brien");
    }

    [Fact]
    public void literal_has_no_already_quoted_shortcut()
    {
        // weasel#416's postmortem: an "is this already escaped?" test cannot be made safely from the
        // shape of untrusted input — a value that happens to start and end with a quote would skip
        // escaping entirely, which is strictly worse than the missing escape it replaces. Formatting
        // here is unconditional, so a quote-wrapped input is escaped like any other.
        SqlEscaping.Literal("'sneaky'").ShouldBe("'''sneaky'''");
    }

    [Fact]
    public void a_name_bound_for_both_positions_composes_the_two_escapes()
    {
        // The audit's specific trap: the same object name appears bare in `ALTER TABLE [s].[t]` and
        // as a string in `OBJECT_ID('[s].[t]')`. Those need different escapes, applied in order.
        var qualified = SqlEscaping.QualifiedName("sch'ema", "tab]le");
        qualified.ShouldBe("[sch'ema].[tab]]le]");
        SqlEscaping.Literal(qualified).ShouldBe("'[sch''ema].[tab]]le]'");
    }
}
