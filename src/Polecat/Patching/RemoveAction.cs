namespace Polecat.Patching;

/// <summary>
///     Controls the behavior when removing elements from a child collection.
/// </summary>
public enum RemoveAction
{
    /// <summary>
    ///     Remove only the first matching element.
    /// </summary>
    RemoveFirst = 0,

    /// <summary>
    ///     Remove all matching elements.
    /// </summary>
    RemoveAll = 1
}
