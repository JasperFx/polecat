namespace Polecat.TestUtils;

public static class DocumentStoreExtensions
{
    extension(DocumentStore Store)
    {
        /// <summary>
        ///     Start every projection agent and wait until each has reached the high water mark.
        /// </summary>
        /// <remarks>
        ///     This used to retry the start and catch-up up to three times. The failure it was covering for was
        ///     jasperfx#1231: <c>CatchUpAsync</c> stops the running agents first, and one still loading a page
        ///     failed the catch-up with "The channel has been closed". JasperFx 3.7.1 waits for that page load.
        /// </remarks>
        public async Task WaitForProjectionAsync()
        {
            using var daemon = await Store.BuildProjectionDaemonAsync();
            await daemon.StartAllAsync();
            await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        }
    }
}
