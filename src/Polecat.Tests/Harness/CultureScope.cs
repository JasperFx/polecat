using System.Globalization;

namespace Polecat.Tests.Harness;

/// <summary>
///     Switch the culture for one block of a test and put it back. Both properties are per-thread and
///     flow with the async context, so a fact that forgets to restore them changes the next fact on
///     that thread rather than failing itself.
/// </summary>
/// <remarks>
///     Declare it with <c>using</c> at the top of the block that needs it. A store built, or a query
///     translated, before the scope opens has already formatted whatever it was going to format under
///     the old culture, and that is the mistake this helper cannot catch for you.
/// </remarks>
public sealed class CultureScope : IDisposable
{
    private readonly CultureInfo _culture = CultureInfo.CurrentCulture;
    private readonly CultureInfo _uiCulture = CultureInfo.CurrentUICulture;

    private CultureScope()
    {
    }

    /// <summary>
    ///     Formatting culture only: what <c>ToString</c>, <c>ToLower</c> and <c>string.Format</c> see.
    /// </summary>
    public static CultureScope Formatting(string name)
    {
        var scope = new CultureScope();
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
        return scope;
    }

    /// <summary>
    ///     Resource culture only: which satellite assembly a library's exception messages come from.
    /// </summary>
    public static CultureScope Resources(string name)
    {
        var scope = new CultureScope();
        CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(name);
        return scope;
    }

    public void Dispose()
    {
        CultureInfo.CurrentCulture = _culture;
        CultureInfo.CurrentUICulture = _uiCulture;
    }
}
