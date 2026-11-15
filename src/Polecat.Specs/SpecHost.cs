using Bobcat.CritterStack;
using JasperFx;
using JasperFx.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.TestUtils;

// A store-only act attributes events by stream, and a stream another spec starts at the same moment
// would look like the act's, so these specs run one at a time.
[assembly: Xunit.v3.Parallelization(Mode = Xunit.Sdk.ParallelMode.None)]

namespace Polecat.Specs;

/// <summary>One Polecat host for every spec, in a schema of its own.</summary>
public class SpecHost : IAsyncLifetime
{
    public IHost Host { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "polecat_specs";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Events.StreamIdentity = StreamIdentity.AsGuid;
        });

        Host = builder.Build();
        await Host.StartAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
    }
}

[CollectionDefinition("Polecat")]
public class PolecatCollection : ICollectionFixture<SpecHost>;

/// <summary>Every spec starts from an empty store.</summary>
[Collection("Polecat")]
public abstract class PolecatSpec(SpecHost app) : CritterStackSpec(app.Host), IAsyncLifetime
{
    public async ValueTask InitializeAsync() => await ResetAsync();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
