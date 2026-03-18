using JasperFx.Events.Daemon;

namespace Polecat.Events.TestSupport;

/// <summary>
///     Test harness for verifying projection behavior. Allows you to append events
///     and assert against the projected documents in a structured, step-by-step fashion.
/// </summary>
public partial class ProjectionScenario
{
    private readonly Queue<ScenarioStep> _steps = new();
    private readonly DocumentStore _store;

    public ProjectionScenario(DocumentStore store)
    {
        _store = store;
    }

    internal IProjectionDaemon? Daemon { get; private set; }

    internal ScenarioStep? NextStep => _steps.Count != 0 ? _steps.Peek() : null;

    internal IDocumentSession Session { get; private set; } = null!;

    /// <summary>
    ///     Disable the scenario from cleaning out any existing
    ///     event and projected document data before running the scenario.
    /// </summary>
    public bool DoNotDeleteExistingData { get; set; }

    /// <summary>
    ///     Opt into applying this scenario to a specific tenant id in the
    ///     case of using multi-tenancy of any kind.
    /// </summary>
    public string? TenantId { get; set; }

    internal Task WaitForNonStaleData()
    {
        if (Daemon == null)
        {
            return Task.CompletedTask;
        }

        return Daemon.WaitForNonStaleData(TimeSpan.FromSeconds(30));
    }

    private ScenarioStep Action(Action<IEventOperations> action)
    {
        var step = new ScenarioAction(action);
        _steps.Enqueue(step);
        return step;
    }

    private ScenarioStep Assertion(Func<IQuerySession, CancellationToken, Task> check)
    {
        var step = new ScenarioAssertion(check);
        _steps.Enqueue(step);
        return step;
    }

    internal async Task Execute(CancellationToken ct = default)
    {
        if (!DoNotDeleteExistingData)
        {
            await _store.Advanced.CleanAllEventDataAsync(ct);
            // Clean projected document types
            foreach (var source in _store.Options.Projections.All)
            {
                foreach (var storageType in source.Options.StorageTypes)
                {
                    await CleanDocumentsByTypeAsync(storageType, ct);
                }
            }
        }

        if (_store.Options.Projections.HasAnyAsyncProjections())
        {
            Daemon = await _store.BuildProjectionDaemonAsync();
            await Daemon.StartAllAsync();
        }

        Session = !string.IsNullOrEmpty(TenantId)
            ? _store.LightweightSession(new SessionOptions { TenantId = TenantId })
            : _store.LightweightSession();

        try
        {
            var exceptions = new List<Exception>();
            var number = 0;
            var descriptions = new List<string>();

            while (_steps.Count > 0)
            {
                number++;
                var step = _steps.Dequeue();

                try
                {
                    await step.Execute(this, ct);
                    descriptions.Add($"{number.ToString().PadLeft(3)}. {step.Description}");
                }
                catch (Exception e)
                {
                    descriptions.Add($"FAILED: {number.ToString().PadLeft(3)}. {step.Description}");
                    descriptions.Add(e.ToString());
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 0)
            {
                throw new ProjectionScenarioException(descriptions, exceptions);
            }
        }
        finally
        {
            if (Daemon != null)
            {
                await Daemon.StopAllAsync();
                if (Daemon is IAsyncDisposable disposable)
                {
                    await disposable.DisposeAsync();
                }
            }

            await Session.DisposeAsync();
        }
    }

    private async Task CleanDocumentsByTypeAsync(Type type, CancellationToken ct)
    {
        var provider = _store.GetProvider(type);
        var tableName = provider.Mapping.QualifiedTableName;

        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(_store.Options.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DELETE FROM {tableName};";
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
