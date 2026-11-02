namespace Polecat.Internal;

/// <summary>
///     #896: reports a cancelled database operation the way a caller expects to hear about it.
/// </summary>
/// <remarks>
///     <para>
///         SqlClient does not surface a cancelled command as an <see cref="OperationCanceledException" />.
///         Depending on how far the command had got it throws a <c>SqlException</c>, either "Operation
///         cancelled by user" or "A severe error occurred on the current command", or a plain
///         <see cref="InvalidOperationException" /> from <c>SQL.OperationCancelled()</c> if it had not reached
///         the server yet. Anything that catches only <see cref="OperationCanceledException" />, which is
///         what a caller who passed a token reasonably does, then reads a cancel as a failure.
///     </para>
///     <para>
///         The test is on the token and not on the exception's type or message. By the time the token has
///         been cancelled the operation is over either way and the caller asked for that; the original stays
///         on <see cref="Exception.InnerException" />. While the token has not been cancelled nothing is
///         touched, so a real failure never reads as a cancel.
///     </para>
/// </remarks>
internal static class SqlCancellation
{
    /// <param name="work">The operation in progress.</param>
    /// <param name="operation">What to call it in the message: "{operation} was cancelled."</param>
    /// <param name="token">The token <paramref name="work" /> was given.</param>
    public static async Task TranslatingCancellation(this Task work, string operation, CancellationToken token)
    {
        try
        {
            await work.ConfigureAwait(false);
        }
        catch (Exception ex) when (token.IsCancellationRequested && ex is not OperationCanceledException)
        {
            throw new OperationCanceledException($"{operation} was cancelled.", ex, token);
        }
    }

    /// <inheritdoc cref="TranslatingCancellation(Task, string, CancellationToken)" />
    public static async Task<T> TranslatingCancellation<T>(this Task<T> work, string operation,
        CancellationToken token)
    {
        try
        {
            return await work.ConfigureAwait(false);
        }
        catch (Exception ex) when (token.IsCancellationRequested && ex is not OperationCanceledException)
        {
            throw new OperationCanceledException($"{operation} was cancelled.", ex, token);
        }
    }
}
