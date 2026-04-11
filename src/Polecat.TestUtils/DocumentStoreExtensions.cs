using Microsoft.Data.SqlClient;

namespace Polecat.TestUtils;

public static class DocumentStoreExtensions
{
    extension(DocumentStore Store)
    {
        public async Task WaitForProjectionAsync()
        {
            SqlConnection.ClearAllPools();
            using var daemon = await Store.BuildProjectionDaemonAsync();
            await daemon.StartAllAsync();

            // Retry on transient SQL Server connection errors from daemon internals
            for (var attempt = 0; attempt < 3; attempt++)
            {
                try
                {
                    await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
                    return;
                }
                catch (AggregateException) when (attempt < 2)
                {
                    SqlConnection.ClearAllPools();
                    await Task.Delay(200);
                }
            }
        }
    }
}
