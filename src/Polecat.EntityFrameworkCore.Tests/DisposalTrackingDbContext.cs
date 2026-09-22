using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     A <see cref="TestDbContext" /> that counts its own construction against its own disposal.
/// </summary>
/// <remarks>
///     #650: the leak this exists to catch is counted here rather than in
///     <c>sys.dm_exec_sessions</c> on purpose. The reported symptom is a leaked connection per
///     batch, but that only surfaces when the context owns its connection — on Polecat's SQL Server
///     path the placeholder is never opened, so a backend count would report nothing while the
///     defect underneath was fully present. Counting construction against disposal pins the defect
///     itself: no pool timing, no sampling, no dependence on the server.
/// </remarks>
public class DisposalTrackingDbContext : TestDbContext
{
    private static int _created;
    private static int _disposed;

    public DisposalTrackingDbContext(DbContextOptions<DisposalTrackingDbContext> options)
        : base(Rebuild(options))
    {
        Interlocked.Increment(ref _created);
    }

    public static int Created => Volatile.Read(ref _created);
    public static int Disposed => Volatile.Read(ref _disposed);

    /// <summary>
    ///     The base ctor takes <c>DbContextOptions&lt;TestDbContext&gt;</c>, and EF Core hands a
    ///     subclass its own closed options type. Re-wrapping is the whole of the adaptation.
    /// </summary>
    private static DbContextOptions<TestDbContext> Rebuild(DbContextOptions<DisposalTrackingDbContext> options)
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();
        foreach (var extension in options.Extensions)
        {
            ((Microsoft.EntityFrameworkCore.Infrastructure.IDbContextOptionsBuilderInfrastructure)builder)
                .AddOrUpdateExtension(extension);
        }

        return builder.Options;
    }

    public static void Reset()
    {
        Interlocked.Exchange(ref _created, 0);
        Interlocked.Exchange(ref _disposed, 0);
    }

    public override void Dispose()
    {
        Interlocked.Increment(ref _disposed);
        base.Dispose();
    }

    public override ValueTask DisposeAsync()
    {
        Interlocked.Increment(ref _disposed);
        return base.DisposeAsync();
    }
}
