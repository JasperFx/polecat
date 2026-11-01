using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     SQL text and fragments rendered in sequence — how something that may bind a parameter ends up
///     inside a larger expression, as in <c>COUNT(CASE WHEN &lt;predicate&gt; THEN 1 END)</c>.
/// </summary>
/// <remarks>
///     Each part is either a <see cref="string" /> of trusted SQL or an <see cref="ISqlFragment" />. A
///     runtime value never belongs in a string part: it goes through a fragment, which binds it.
/// </remarks>
internal sealed class CompositeSqlFragment : ISqlFragment
{
    private readonly object[] _parts;

    public CompositeSqlFragment(params object[] parts)
    {
        _parts = parts;
    }

    public void Apply(ICommandBuilder builder)
    {
        foreach (var part in _parts)
        {
            if (part is ISqlFragment fragment)
            {
                fragment.Apply(builder);
            }
            else
            {
                builder.Append((string)part);
            }
        }
    }
}
