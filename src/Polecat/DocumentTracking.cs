namespace Polecat;

/// <summary>
///     Controls the identity map behavior of a document session.
/// </summary>
public enum DocumentTracking
{
    /// <summary>
    ///     No identity map or change tracking. Lightweight and fast.
    /// </summary>
    None,

    /// <summary>
    ///     Maintains an identity map so that loading the same document by id
    ///     within a session returns the same object reference.
    /// </summary>
    IdentityOnly
}
