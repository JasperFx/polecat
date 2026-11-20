using System.Security.Cryptography;
using System.Text;
using JasperFx.Core.Reflection;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     marten#6003 / #1053: the slice index of one multi-stream projection version that opted in with
///     <c>UseSliceIndex()</c>. One row for every (tenant, identity, event) the daemon applied, written in the batch
///     that applied the event, so one identity's events can be found again whatever the projection's grouping rules.
/// </summary>
/// <remarks>
///     <para>
///         One table per projection name and version (<see cref="TableNameFor" />), with the identity in its own
///         column type. Keyed by (tenant_id, identity_value, seq_id). Pruning and rewinds go by <c>seq_id</c>, which
///         has an index of its own.
///     </para>
///     <para>
///         No foreign key to <c>pc_events</c>: it would cost every delete of an event, and a row whose event was
///         compacted or tombstoned is ignored when the index is read.
///     </para>
/// </remarks>
internal class SliceIndexTable : Polecat.Storage.KeyedTable
{
    public const string Prefix = "pc_slice_index_";

    public SliceIndexTable(EventGraph eventGraph, string projectionName, uint version, Type identityType)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableNameFor(projectionName, version)))
    {
        var simple = SimpleIdentityType(identityType);

        AddColumn("tenant_id", eventGraph.KeyText()).AsPrimaryKey().NotNull();
        AddColumn("identity_value", EventTagTable.SqlServerTypeFor(simple, eventGraph.UnicodeKeys)).AsPrimaryKey()
            .NotNull();
        AddColumn("seq_id", "bigint").AsPrimaryKey().NotNull();

        var name = Identifier.Name;
        PrimaryKeyName = $"pk_{name}";

        // Two string keys can pass SQL Server's 900 byte limit for a clustered key with unicode keys (#935)
        if (simple == typeof(string) && eventGraph.UnicodeKeys)
        {
            PrimaryKeyIsClustered = false;
        }

        Indexes.Add(new IndexDefinition($"ix_{name}_seq_id") { Columns = ["seq_id"] });
    }

    /// <summary>
    ///     The table for one projection version: <c>pc_slice_index_{name}</c> for version 1 and
    ///     <c>pc_slice_index_{name}_v{version}</c> after it. The name is lower-cased with anything but letters, digits
    ///     and underscores replaced, and a long name is cut and given a hash so it stays under SQL Server's 128
    ///     characters (with room for the index names built from it).
    /// </summary>
    public static string TableNameFor(string projectionName, uint version)
    {
        var builder = new StringBuilder(projectionName.Length);
        foreach (var c in projectionName.ToLowerInvariant())
        {
            builder.Append(c is >= 'a' and <= 'z' or >= '0' and <= '9' or '_' ? c : '_');
        }

        var name = builder.ToString();
        if (name.Length > 80)
        {
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(projectionName)))[..8]
                .ToLowerInvariant();
            name = $"{name[..71]}_{hash}";
        }

        return version <= 1 ? Prefix + name : $"{Prefix}{name}_v{version}";
    }

    /// <summary>The value an identity is stored as: Guid, string, int or long, or what a strong-typed id wraps.</summary>
    public static Type SimpleIdentityType(Type identityType)
    {
        var type = Nullable.GetUnderlyingType(identityType) ?? identityType;
        if (type == typeof(Guid) || type == typeof(string) || type == typeof(int) || type == typeof(long)) return type;

        if (StrongTypedIdResolver.TryResolve(type, out var info, StrongTypedIdStyle.AnyType))
        {
            return info.SimpleType;
        }

        throw new ArgumentOutOfRangeException(nameof(identityType),
            $"A slice index can't store an identity of type {identityType.FullNameInCode()}");
    }

    /// <summary>
    ///     Every async multi-stream projection of the store that opted into the slice index, with its table. Composite
    ///     members don't park, so they have none.
    /// </summary>
    public static IEnumerable<(string ProjectionName, uint Version, Type IdentityType)> IndexedProjections(
        StoreOptions options)
    {
        foreach (var projection in options.Projections.All)
        {
            if (projection is not IAggregateProjection { Scope: AggregationScope.MultiStream } aggregate) continue;
            if (projection.Lifecycle != ProjectionLifecycle.Async || projection.Options.SliceIndex == null) continue;

            yield return (projection.Name, projection.Version, aggregate.IdentityType);
        }
    }

    /// <summary>The qualified name of every slice index table the store declares.</summary>
    public static IReadOnlyList<string> QualifiedTableNames(StoreOptions options)
        => IndexedProjections(options)
            .Select(x => Polecat.Internal.SqlEscaping.QualifiedName(options.EventGraph.DatabaseSchemaName,
                TableNameFor(x.ProjectionName, x.Version)))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
}
