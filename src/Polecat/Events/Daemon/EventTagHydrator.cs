using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Events.Daemon;

/// <summary>
///     marten#6003 / #1053: reads the DCB tags of events the daemon loaded, from the <c>pc_event_tag_*</c> tables, for
///     a projection that slices by them (<c>IdentityFromTag</c>, which sets <c>IncludeEventTags</c>). Tags are written
///     at append and never read back otherwise, so without this an async projection grouped by tag sees no tags.
/// </summary>
/// <remarks>
///     One read per registered tag type over the page's sequence range. Under conjoined tenancy a tag row is matched to
///     its event by tenant and sequence, because tenant-partitioned events number each tenant's events on their own.
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2072:DynamicallyAccessedMembers",
    Justification = "The tag types are registered with the store, which preserves them.")]
internal static class EventTagHydrator
{
    private static readonly ConcurrentDictionary<Type, Func<object, object>> _wrappers = new();

    public static async Task HydrateAsync(string connectionString, EventGraph events, IReadOnlyList<IEvent> loaded,
        string? tenantId, CancellationToken token)
    {
        if (loaded.Count == 0 || events.TagTypes.Count == 0) return;

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        await HydrateAsync(conn, events, loaded, tenantId, token).ConfigureAwait(false);
    }

    public static async Task HydrateAsync(SqlConnection conn, EventGraph events, IReadOnlyList<IEvent> loaded,
        string? tenantId, CancellationToken token)
    {
        if (loaded.Count == 0 || events.TagTypes.Count == 0) return;

        var conjoined = events.TenancyStyle == TenancyStyle.Conjoined;
        var floor = long.MaxValue;
        var ceiling = long.MinValue;
        var bySequence = new Dictionary<(string Tenant, long Sequence), IEvent>(loaded.Count);
        foreach (var @event in loaded)
        {
            floor = Math.Min(floor, @event.Sequence);
            ceiling = Math.Max(ceiling, @event.Sequence);
            bySequence[(conjoined ? @event.TenantId : string.Empty, @event.Sequence)] = @event;
        }

        foreach (var registration in events.TagTypes)
        {
            var wrap = wrapperFor(registration);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = conjoined
                ? $"SELECT value, seq_id, tenant_id FROM {tableFor(events, registration)} WHERE seq_id >= @floor AND seq_id <= @ceiling{(tenantId == null ? "" : " AND tenant_id = @tenant")} ORDER BY seq_id;"
                : $"SELECT value, seq_id FROM {tableFor(events, registration)} WHERE seq_id >= @floor AND seq_id <= @ceiling ORDER BY seq_id;";
            cmd.Parameters.Add("@floor", SqlDbType.BigInt).Value = floor;
            cmd.Parameters.Add("@ceiling", SqlDbType.BigInt).Value = ceiling;
            if (conjoined && tenantId != null) cmd.Parameters.AddKey("@tenant", tenantId, events.UnicodeKeys);

            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var key = (conjoined ? reader.GetString(2) : string.Empty, reader.GetInt64(1));
                if (!bySequence.TryGetValue(key, out var @event)) continue;

                var tag = wrap(reader.GetValue(0));
                if (@event.Tags is { Count: > 0 } tags && tags.Any(x => x.TagType == registration.TagType && Equals(x.Value, tag)))
                {
                    continue;
                }

                @event.AddTag(new EventTag(registration.TagType, tag));
            }
        }
    }

    /// <summary>
    ///     #1053: a tag back from its text, as an <c>EventRecord</c> carries it (keyed by the tag type's name or table
    ///     suffix), so stepping an <c>IdentityFromTag</c> projection from records sees its tags.
    /// </summary>
    public static object TagFromText(ITagTypeRegistration registration, string text)
    {
        var simple = registration.SimpleType;
        object value = simple == typeof(Guid) ? Guid.Parse(text)
            : simple == typeof(int) ? int.Parse(text, System.Globalization.CultureInfo.InvariantCulture)
            : simple == typeof(long) ? long.Parse(text, System.Globalization.CultureInfo.InvariantCulture)
            : simple == typeof(short) ? short.Parse(text, System.Globalization.CultureInfo.InvariantCulture)
            : text;

        return wrapperFor(registration)(value);
    }

    private static string tableFor(EventGraph events, ITagTypeRegistration registration)
        => Polecat.Internal.SqlEscaping.QualifiedName(events.DatabaseSchemaName, EventGraph.TagTableNameFor(registration));

    // The stored simple value back into the tag type the projection slices by
    private static Func<object, object> wrapperFor(ITagTypeRegistration registration)
        => _wrappers.GetOrAdd(registration.TagType, static (tagType, simpleType) =>
        {
            if (tagType == simpleType) return value => value;

            if (StrongTypedIdResolver.TryResolve(tagType, out var info, StrongTypedIdStyle.AnyType))
            {
                return value => StrongTypedIdResolver.Wrap(info, Convert.ChangeType(value, info.SimpleType,
                    System.Globalization.CultureInfo.InvariantCulture));
            }

            throw new InvalidOperationException(
                $"Polecat can't read tags of type {tagType.FullNameInCode()} back for an async projection: it is not "
                + $"{simpleType.FullNameInCode()} or a strong-typed identifier wrapping one.");
        }, registration.SimpleType);
}
