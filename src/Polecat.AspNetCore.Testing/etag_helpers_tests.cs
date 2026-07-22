using Microsoft.AspNetCore.Http;
using Polecat.AspNetCore;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

/// <summary>
///     Pure-HTTP unit coverage for <see cref="ETagHelpers"/> (marten#5015 parity, polecat#356):
///     the <c>*</c> wildcard, <c>W/</c> weak validators, and multi-value comma lists.
/// </summary>
public class etag_helpers_tests
{
    private static HttpContext ContextWith(params string[] ifNoneMatch)
    {
        var context = new DefaultHttpContext();
        if (ifNoneMatch.Length > 0)
        {
            context.Request.Headers["If-None-Match"] = ifNoneMatch;
        }

        return context;
    }

    [Fact]
    public void format_long_produces_quoted_value()
    {
        ETagHelpers.Format(42L).ShouldBe("\"42\"");
    }

    [Fact]
    public void format_guid_produces_quoted_value()
    {
        var id = Guid.NewGuid();
        ETagHelpers.Format(id).ShouldBe($"\"{id}\"");
    }

    [Fact]
    public void no_header_does_not_match()
    {
        ETagHelpers.IfNoneMatchMatches(ContextWith(), "\"5\"").ShouldBeFalse();
    }

    [Fact]
    public void exact_match()
    {
        ETagHelpers.IfNoneMatchMatches(ContextWith("\"5\""), "\"5\"").ShouldBeTrue();
    }

    [Fact]
    public void mismatch_does_not_match()
    {
        ETagHelpers.IfNoneMatchMatches(ContextWith("\"6\""), "\"5\"").ShouldBeFalse();
    }

    [Fact]
    public void wildcard_matches()
    {
        ETagHelpers.IfNoneMatchMatches(ContextWith("*"), "\"5\"").ShouldBeTrue();
    }

    [Fact]
    public void weak_validator_matches_strong_etag()
    {
        // W/ weak-validator prefix is stripped before comparison (RFC 7232 §3.2).
        ETagHelpers.IfNoneMatchMatches(ContextWith("W/\"5\""), "\"5\"").ShouldBeTrue();
    }

    [Fact]
    public void multi_value_comma_list_matches_any()
    {
        ETagHelpers.IfNoneMatchMatches(ContextWith("\"1\", \"5\", \"9\""), "\"5\"").ShouldBeTrue();
    }

    [Fact]
    public void multi_value_comma_list_no_match()
    {
        ETagHelpers.IfNoneMatchMatches(ContextWith("\"1\", \"2\", \"3\""), "\"5\"").ShouldBeFalse();
    }
}
