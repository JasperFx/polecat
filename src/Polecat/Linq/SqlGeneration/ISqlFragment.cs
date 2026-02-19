namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Represents a fragment of SQL that can be applied to a CommandBuilder.
/// </summary>
internal interface ISqlFragment
{
    void Apply(CommandBuilder builder);
}
