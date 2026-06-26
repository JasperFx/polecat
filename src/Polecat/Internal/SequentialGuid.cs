using System.Threading;

namespace Polecat.Internal;

/// <summary>
///     Generates sequential <see cref="Guid" /> values that sort sequentially in SQL Server's
///     <c>uniqueidentifier</c> ordering, avoiding the index fragmentation caused by random
///     <see cref="Guid.NewGuid" /> values used as clustered primary keys (#235).
/// </summary>
/// <remarks>
///     SQL Server orders <c>uniqueidentifier</c> values by their last six bytes first, so a
///     monotonically increasing counter is woven into exactly those bytes. The algorithm mirrors
///     EF Core's <c>SequentialGuidValueGenerator</c>
///     (https://github.com/dotnet/efcore/blob/main/src/EFCore/ValueGeneration/SequentialGuidValueGenerator.cs)
///     so Polecat-assigned ids interleave cleanly with EF-assigned ones in the same table.
/// </remarks>
internal static class SequentialGuid
{
    private static long _counter = System.DateTime.UtcNow.Ticks;

    /// <summary>
    ///     Creates a new sequential <see cref="Guid" />.
    /// </summary>
    public static Guid NewGuid()
    {
        var guidBytes = Guid.NewGuid().ToByteArray();
        var counterBytes = BitConverter.GetBytes(Interlocked.Increment(ref _counter));

        if (!BitConverter.IsLittleEndian)
        {
            Array.Reverse(counterBytes);
        }

        guidBytes[08] = counterBytes[1];
        guidBytes[09] = counterBytes[0];
        guidBytes[10] = counterBytes[7];
        guidBytes[11] = counterBytes[6];
        guidBytes[12] = counterBytes[5];
        guidBytes[13] = counterBytes[4];
        guidBytes[14] = counterBytes[3];
        guidBytes[15] = counterBytes[2];

        return new Guid(guidBytes);
    }
}
