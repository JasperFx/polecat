using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.Data.SqlClient;
using Serilog;

/// <summary>
/// marten#6069 on SQL Server: a gate's tracks held across processes, so two LocalGate runs from different
/// worktrees with the same <c>--lane-prefix</c> queue on the lane catalogs they share instead of dropping each
/// other's.
/// </summary>
/// <remarks>
/// <para>
/// Bobcat keeps two units on one track apart inside one gate process only. Each track here is also a
/// session-owned application lock (<c>sp_getapplock</c>) in the server's <c>master</c> database, taken when a
/// unit holding the track starts and released when that unit finishes. Gates with different lane prefixes
/// have different tracks and never wait on each other.
/// </para>
/// <para>
/// A unit's locks live on one unpooled connection of their own. Closing it ends the session, which releases
/// them, and so does the gate process dying. Locks are taken in name order, the same order in every gate, so
/// two gates waiting on each other's tracks can't deadlock.
/// </para>
/// <para>
/// Bobcat 1.0.0 has no hook for the end of a unit, so the release is driven by the gate's own log line for it:
/// <c>=== {unit}: ...</c>, written once for every unit that started, whatever its outcome.
/// </para>
/// </remarks>
sealed class GateTrackLocks : IDisposable
{
    const string ResourcePrefix = "polecat-localgate-track:";

    readonly string _server;
    readonly Dictionary<string, SqlConnection> _held = new(StringComparer.Ordinal);
    readonly object _gate = new();

    public GateTrackLocks(string serverConnectionString)
    {
        _server = new SqlConnectionStringBuilder(serverConnectionString)
        {
            // Application locks belong to a database, and the catalogs a track stands for are the server's, so
            // every gate takes them in master
            InitialCatalog = "master",

            // A pooled connection would outlive Close, and its session's locks with it
            Pooling = false,
            ApplicationName = "polecat-localgate-track-lock"
        }.ConnectionString;
    }

    /// <summary>The application lock resource of a track: the same in every process.</summary>
    public static string ResourceFor(string track) => ResourcePrefix + track.ToLowerInvariant();

    /// <summary>
    /// Take every track of <paramref name="unit"/>, waiting for another gate that holds one. Blocks the
    /// unit's start, which is what the wait is for.
    /// </summary>
    public void Acquire(string unit, IReadOnlyCollection<string> tracks)
    {
        if (tracks.Count == 0) return;

        var conn = new SqlConnection(_server);
        conn.Open();

        try
        {
            foreach (var track in tracks.Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.Ordinal))
            {
                if (getAppLock(conn, track, timeoutMilliseconds: 0)) continue;

                Log.Information("LocalGate: {Unit} waits for track {Track}, which a gate in another checkout holds", unit, track);
                if (!getAppLock(conn, track, timeoutMilliseconds: -1))
                {
                    throw new InvalidOperationException($"LocalGate: could not take track {track} for {unit}");
                }

                Log.Information("LocalGate: {Unit} has track {Track}", unit, track);
            }
        }
        catch
        {
            conn.Dispose();
            throw;
        }

        lock (_gate)
        {
            // A unit name starts once per gate; a leftover from an unreported finish is released here
            if (_held.Remove(unit, out var stale)) stale.Dispose();
            _held[unit] = conn;
        }
    }

    /// <summary>Release the tracks <paramref name="unit"/> holds, if any.</summary>
    public void Release(string unit)
    {
        SqlConnection conn;
        lock (_gate)
        {
            if (!_held.Remove(unit, out conn)) return;
        }

        // Ending the session releases its application locks
        conn.Dispose();
    }

    /// <summary>
    /// Release the tracks of the unit a gate log line reports as finished: <c>=== {unit}: {mark} ...</c>
    /// or <c>=== {unit}: ? {exception}</c>, but not <c>=== {unit}: starting on ...</c>.
    /// </summary>
    public void ReleaseIfFinished(string gateLogLine)
    {
        if (!gateLogLine.StartsWith("=== ", StringComparison.Ordinal)) return;

        string[] holding;
        lock (_gate) holding = _held.Keys.ToArray();

        if (FinishedUnit(gateLogLine, holding) is { } unit) Release(unit);
    }

    /// <summary>The unit of <paramref name="units" /> a gate log line reports as finished, or null.</summary>
    public static string FinishedUnit(string gateLogLine, IEnumerable<string> units)
    {
        if (!gateLogLine.StartsWith("=== ", StringComparison.Ordinal)) return null;

        foreach (var unit in units)
        {
            var prefix = $"=== {unit}: ";
            if (!gateLogLine.StartsWith(prefix, StringComparison.Ordinal)) continue;
            if (gateLogLine.AsSpan(prefix.Length).StartsWith("starting on ", StringComparison.Ordinal)) return null;
            return unit;
        }

        return null;
    }

    public void Dispose()
    {
        lock (_gate)
        {
            foreach (var conn in _held.Values) conn.Dispose();
            _held.Clear();
        }
    }

    // sp_getapplock returns 0 or 1 when granted, -1 on timeout, below that on an error
    static bool getAppLock(SqlConnection conn, string track, int timeoutMilliseconds)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "sp_getapplock";
        cmd.CommandType = CommandType.StoredProcedure;

        // The wait for another gate is as long as that gate's unit
        cmd.CommandTimeout = 0;

        cmd.Parameters.Add("@Resource", SqlDbType.NVarChar, 255).Value = ResourceFor(track);
        cmd.Parameters.Add("@LockMode", SqlDbType.VarChar, 32).Value = "Exclusive";
        cmd.Parameters.Add("@LockOwner", SqlDbType.VarChar, 32).Value = "Session";
        cmd.Parameters.Add("@LockTimeout", SqlDbType.Int).Value = timeoutMilliseconds;
        var result = cmd.Parameters.Add("@Result", SqlDbType.Int);
        result.Direction = ParameterDirection.ReturnValue;

        cmd.ExecuteNonQuery();

        var granted = (int)result.Value;
        if (granted < -1)
        {
            throw new InvalidOperationException($"sp_getapplock refused track {track} ({granted})");
        }

        return granted >= 0;
    }
}
