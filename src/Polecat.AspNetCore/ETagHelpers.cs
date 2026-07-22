using Microsoft.AspNetCore.Http;

namespace Polecat.AspNetCore;

/// <summary>
/// Helpers for HTTP conditional requests (ETag / If-None-Match → 304 Not Modified). Pure HTTP
/// logic ported near-verbatim from Marten's <c>ETagHelpers</c> (JasperFx/marten#5015): handles the
/// <c>*</c> wildcard, comma-separated <c>If-None-Match</c> lists, and strips <c>W/</c> weak
/// validators. Weak comparison is the correct function for <c>If-None-Match</c> per RFC 7232 §3.2.
/// </summary>
public static class ETagHelpers
{
    /// <summary>
    /// Formats a <see cref="Guid"/> document/stream version as a quoted, opaque ETag value.
    /// </summary>
    public static string Format(Guid version) => $"\"{version}\"";

    /// <summary>
    /// Formats a <see cref="long"/> document/stream version as a quoted, opaque ETag value.
    /// </summary>
    public static string Format(long version) => $"\"{version}\"";

    /// <summary>
    /// Returns <c>true</c> when the request's <c>If-None-Match</c> header matches
    /// <paramref name="etag"/> (weak comparison), or contains the <c>*</c> wildcard. A missing or
    /// empty header returns <c>false</c>.
    /// </summary>
    public static bool IfNoneMatchMatches(HttpContext context, string etag)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Request.Headers.TryGetValue("If-None-Match", out var values))
        {
            return false;
        }

        foreach (var raw in values)
        {
            if (string.IsNullOrWhiteSpace(raw)) continue;

            foreach (var candidate in raw.Split(','))
            {
                var trimmed = candidate.Trim();
                if (trimmed.Length == 0) continue;

                // "*" matches any current representation (RFC 7232 §3.2).
                if (trimmed == "*") return true;

                if (WeakEquals(trimmed, etag)) return true;
            }
        }

        return false;
    }

    // Weak comparison: strip the "W/" weak-validator prefix from both sides before comparing.
    private static bool WeakEquals(string a, string b) =>
        string.Equals(StripWeakPrefix(a), StripWeakPrefix(b), StringComparison.Ordinal);

    private static string StripWeakPrefix(string tag) =>
        tag.StartsWith("W/", StringComparison.Ordinal) ? tag[2..] : tag;
}
