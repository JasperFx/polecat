using Microsoft.Data.SqlClient;

namespace Polecat.TestUtils;

public static class DocumentStoreExtensions
{
    extension(DocumentStore Store)
    {
        public async Task WaitForProjectionAsync()
        {
            const int maxRetries = 3;
            using var daemon = await Store.BuildProjectionDaemonAsync();

            for (var attempt = 0; attempt < maxRetries; attempt++)
            {
                try
                {
                    await daemon.StartAllAsync();
                    await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
                    return;
                }
                catch
                {
                    SqlConnection.ClearAllPools();
                    if (attempt == maxRetries - 1)
                        throw;
                    await Task.Delay(200);
                }
            }
        }
    }
}
