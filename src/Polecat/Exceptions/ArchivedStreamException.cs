namespace Polecat.Exceptions;

/// <summary>
///     Thrown when an append is refused because the stream has been archived.
/// </summary>
/// <remarks>
///     #654 / jasperfx#871. Archiving is reversible bookkeeping rather than deletion, so the refusal
///     names the way back — <c>UnArchiveStream</c> — which is the whole difference between this and
///     the <see cref="InvalidStreamException" /> it replaces: that one stated the reason and stopped,
///     even though <c>UnArchiveStream(Guid|string)</c> has always existed.
///     <para>
///         Derives from <see cref="JasperFx.Events.ArchivedStreamException" />, which the lift made
///         canonical from Fisher's shape, so store-agnostic code catches one type across Marten,
///         Polecat and Fisher. The name stays in <c>Polecat.Exceptions</c> alongside
///         <see cref="StreamLockedException" /> and <see cref="NonExistentStreamException" />.
///     </para>
/// </remarks>
public class ArchivedStreamException : JasperFx.Events.ArchivedStreamException
{
    public ArchivedStreamException(object id) : base(id)
    {
    }
}
