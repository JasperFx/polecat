using JasperFx;
using JasperFx.Linq;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Polecat.TestUtils;

namespace Polecat.Tests.Diagnostics;

public enum CriteriaOrderStatus
{
    Pending = 0,
    Shipped = 1,
    Delivered = 2,
    Cancelled = 3
}

public class CriteriaAddress
{
    public string City { get; set; } = "";
    public string? Region { get; set; }
}

public class CriteriaBillTo
{
    public string? City { get; set; }
}

public class CriteriaLineItem
{
    public string Sku { get; set; } = "";
    public int Qty { get; set; }
}

/// <summary>
///     jasperfx#869: one document with a member of every kind a Dynamic LINQ predicate can reach — dates,
///     an offset, decimals, an enum, a nested object, collections of strings, objects and ints, a Guid, a
///     bool and a nullable string.
/// </summary>
public class CriteriaOrder
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = "";
    public Guid CustomerId { get; set; }
    public DateTime PlacedAt { get; set; }
    public DateTimeOffset Created { get; set; }
    public decimal Total { get; set; }
    public int Quantity { get; set; }
    public CriteriaOrderStatus Status { get; set; }
    public CriteriaAddress ShipTo { get; set; } = new();
    public List<string> Tags { get; set; } = new();
    public List<CriteriaLineItem> Items { get; set; } = new();
    public int[] Lines { get; set; } = [];
    public bool Priority { get; set; }
    public string? Note { get; set; }
    public DateOnly DueDate { get; set; }
    public DateTime? ShippedAt { get; set; }
    public TimeOnly Window { get; set; }
    public int? Rating { get; set; }
    public TimeSpan Duration { get; set; }
    public int? Discount { get; set; }
    public CriteriaBillTo BillTo { get; set; } = new();
}

/// <summary>What Polecat's diagnostics surface does with a shape, as measured against the oracle.</summary>
public enum ShapeVerdict
{
    /// <summary>Translated, and the rows (and their order) agree with LINQ to objects.</summary>
    Matches,

    /// <summary>
    ///     The provider (or Dynamic LINQ's parser) cannot translate it and throws, which the surface turns into
    ///     a refusal.
    /// </summary>
    ProviderRefuses,

    /// <summary>
    ///     The provider would translate it and answer WRONGLY, so <c>DiagnosticsCriteria</c>'s policy refuses
    ///     it first.
    /// </summary>
    PolicyRefuses,

    /// <summary>
    ///     Agrees with the oracle except for letter case: SQL Server compares strings under the column's
    ///     collation, case-insensitively by default, where LINQ to objects is ordinal. Pinned with the
    ///     rows that collation returns, not refused — it is the database's string semantics, not a
    ///     mistranslation.
    /// </summary>
    CaseInsensitiveCollation
}

/// <summary>A Dynamic LINQ shape: where / order-by text and its arguments.</summary>
public sealed record CriteriaShape(string Name, string? Where, string? OrderBy = null, params object?[] Args)
{
    public ShapeVerdict Verdict { get; init; } = ShapeVerdict.Matches;

    /// <summary>The verdict on a store that keeps enums as their names, when it differs.</summary>
    public ShapeVerdict? VerdictWhenEnumsAreNames { get; init; }

    /// <summary>
    ///     The oracle's text when it differs from <see cref="Where" /> — for <c>.Value</c> on a nullable,
    ///     which LINQ to objects throws on for a null row where a database simply does not match it.
    /// </summary>
    public string? OracleWhere { get; init; }

    /// <summary>The rows a <see cref="ShapeVerdict.CaseInsensitiveCollation" /> shape returns, in order.</summary>
    public string[]? CollationRows { get; init; }

    public ShapeVerdict VerdictFor(bool enumsAreNames)
        => enumsAreNames && VerdictWhenEnumsAreNames is { } names ? names : Verdict;

    public override string ToString() => Name;
}

/// <summary>
///     jasperfx#869: the seeded documents and the shapes measured against them. Every shape's expected
///     answer is computed by LINQ to objects running the SAME text — the oracle — never hand-written.
/// </summary>
public static class CriteriaShapes
{
    public static readonly Guid Acme = Guid.Parse("11111111-1111-1111-1111-111111111111");
    public static readonly Guid Globex = Guid.Parse("22222222-2222-2222-2222-222222222222");

    // Labels: o1..o8, fixed Guids, so a measurement line names the rows rather than printing Guids.
    private static readonly Guid[] _ids = Enumerable.Range(1, 8)
        .Select(i => Guid.Parse($"00000000-0000-0000-0000-00000000000{i}")).ToArray();

    public static string Label(Guid id)
    {
        var index = Array.IndexOf(_ids, id);
        return index < 0 ? id.ToString() : $"o{index + 1}";
    }

    public static IReadOnlyList<CriteriaOrder> Documents() =>
    [
        Order(1, "Alpha Co", Acme, new DateTime(2025, 3, 15, 10, 0, 0, DateTimeKind.Utc), 100.50m, 2,
            CriteriaOrderStatus.Shipped, "Austin", ["vip", "rush"], [("A", 1), ("B", 6)], [1, 2, 3], true, null),
        Order(2, "Beta LLC", Globex, new DateTime(2025, 7, 1, 8, 30, 0, DateTimeKind.Utc), 9.5m, 10,
            CriteriaOrderStatus.Pending, "Boston", ["new"], [("A", 2)], [4], false, "call first"),
        Order(3, "alpha partners", Acme, new DateTime(2024, 3, 15, 23, 0, 0, DateTimeKind.Utc), 1000m, 1,
            CriteriaOrderStatus.Delivered, "austin", [], [], [], false, null),
        Order(4, "Gamma", Globex, new DateTime(2025, 12, 31, 12, 0, 0, DateTimeKind.Utc), 25m, 3,
            CriteriaOrderStatus.Cancelled, "Chicago", ["vip"], [("C", 3), ("D", 4), ("E", 5)], [5, 6], true,
            "gift"),
        Order(5, "Delta", Acme, new DateTime(2023, 1, 9, 0, 0, 0, DateTimeKind.Utc), 50.25m, 20,
            CriteriaOrderStatus.Shipped, "Denver", ["rush"], [("F", 10)], [7, 8, 9, 10], false, null),
        Order(6, "Epsilon", Globex, new DateTime(2025, 3, 2, 16, 45, 0, DateTimeKind.Utc), 100.5m, 2,
            CriteriaOrderStatus.Delivered, "Austin", ["VIP"], [("G", 1), ("H", 1)], [11], false, ""),
        Order(7, "Zeta", Acme, new DateTime(2026, 6, 15, 6, 0, 0, DateTimeKind.Utc), 0.99m, 7,
            CriteriaOrderStatus.Pending, "El Paso", ["vip", "new", "rush"], [("I", 9)], [12, 13], true, null),
        Order(8, "eta", Globex, new DateTime(2025, 3, 15, 0, 0, 0, DateTimeKind.Utc), 250m, 5,
            CriteriaOrderStatus.Shipped, "Fresno", [], [("J", 2), ("K", 2), ("L", 2), ("M", 2)], [], false,
            "leave at door")
    ];

    private static CriteriaOrder Order(int i, string name, Guid customer, DateTime placed, decimal total, int qty,
        CriteriaOrderStatus status, string city, string[] tags, (string, int)[] items, int[] lines, bool priority,
        string? note)
        => new()
        {
            Id = _ids[i - 1],
            CustomerName = name,
            CustomerId = customer,
            PlacedAt = placed,
            Created = new DateTimeOffset(placed).ToOffset(TimeSpan.FromHours(i % 2 == 0 ? -5 : 2)),
            Total = total,
            Quantity = qty,
            Status = status,
            ShipTo = new CriteriaAddress { City = city, Region = i % 3 == 0 ? null : "R" + i },
            Tags = tags.ToList(),
            Items = items.Select(x => new CriteriaLineItem { Sku = x.Item1, Qty = x.Item2 }).ToList(),
            Lines = lines,
            Priority = priority,
            Note = note,
            DueDate = DateOnly.FromDateTime(placed).AddDays(i * 10),
            ShippedAt = i % 2 == 0 ? placed.AddDays(2) : null,
            Window = new TimeOnly(i + 8, i * 5),
            Rating = i % 3 == 0 ? null : i,
            Duration = TimeSpan.FromMinutes(i * 25),
            Discount = i % 4 == 0 ? null : i % 2 == 0 ? 5 : 10,
            BillTo = new CriteriaBillTo { City = i % 3 == 1 ? null : i % 2 == 0 ? "Austin" : "Dallas" }
        };

    /// <summary>
    ///     Every shape the brief names, plus the orderings — an ordering that translates as TEXT would
    ///     sort <c>9.5</c> after <c>100.50</c>, so each typed ordering is a shape of its own.
    /// </summary>
    public static IReadOnlyList<CriteriaShape> All { get; } =
    [
        new("date_year", "PlacedAt.Year = @0", null, 2025) { Verdict = ShapeVerdict.PolicyRefuses },
        new("date_month", "PlacedAt.Month = @0", null, 3) { Verdict = ShapeVerdict.PolicyRefuses },
        new("date_day", "PlacedAt.Day = @0", null, 15) { Verdict = ShapeVerdict.PolicyRefuses },
        new("date_date", "PlacedAt.Date = @0", null, new DateTime(2025, 3, 15, 0, 0, 0, DateTimeKind.Utc)) { Verdict = ShapeVerdict.PolicyRefuses },
        new("offset_year", "Created.Year = @0", null, 2025) { Verdict = ShapeVerdict.PolicyRefuses },
        new("list_count_property", "Items.Count > @0", null, 1) { Verdict = ShapeVerdict.PolicyRefuses },
        new("array_length_property", "Lines.Length > @0", null, 2) { Verdict = ShapeVerdict.PolicyRefuses },
        new("tags_count_property", "Tags.Count = @0", null, 0) { Verdict = ShapeVerdict.PolicyRefuses },
        new("count_method", "Items.Count() > @0", null, 1) { Verdict = ShapeVerdict.ProviderRefuses },
        new("count_method_predicate", "Items.Count(Qty > @0) > 0", null, 2) { Verdict = ShapeVerdict.ProviderRefuses },
        new("any_predicate", "Items.Any(Qty > @0)", null, 5) { Verdict = ShapeVerdict.ProviderRefuses },
        new("any_empty", "Items.Any()", null) { Verdict = ShapeVerdict.ProviderRefuses },
        new("all_predicate", "Items.All(Qty = @0)", null, 2) { Verdict = ShapeVerdict.ProviderRefuses },
        new("tags_any_it", "Tags.Any(it = \"vip\")", null) { Verdict = ShapeVerdict.ProviderRefuses },
        new("tags_contains", "Tags.Contains(@0)", null, "vip") { Verdict = ShapeVerdict.CaseInsensitiveCollation, CollationRows = ["o1", "o4", "o6", "o7"] },
        new("inline_in_ints", "Quantity in (1, 2, 3)", null),
        new("inline_in_strings", "CustomerName in (\"Gamma\", \"Delta\")", null),
        new("in_argument", "Quantity in @0", null, new[] { 1, 2, 3 }),
        new("decimal_equals", "Total = @0", null, 100.50m),
        new("decimal_greater", "Total > @0", null, 100m),
        new("decimal_less", "Total < @0", null, 50.25m),
        new("decimal_literal", "Total >= 100.5m", null),
        new("enum_by_name", "Status = \"Shipped\"", null),
        new("enum_by_name_argument", "Status = @0", null, "Shipped"),
        new("enum_by_number", "Status = 2", null),
        new("enum_by_number_argument", "Status = @0", null, 2),
        new("enum_not_equal", "Status != \"Pending\"", null),
        new("enum_greater_number", "Status > @0", null, 1) { VerdictWhenEnumsAreNames = ShapeVerdict.PolicyRefuses },
        new("enum_greater_name", "Status >= \"Delivered\"", null) { VerdictWhenEnumsAreNames = ShapeVerdict.PolicyRefuses },
        new("enum_in_numbers", "Status in @0", null, new[] { 1, 2 }),
        new("enum_in_names", "Status in @0", null, new object[] { "Shipped", "Delivered" }) { Verdict = ShapeVerdict.ProviderRefuses },
        new("nested_member", "ShipTo.City = @0", null, "Austin") { Verdict = ShapeVerdict.CaseInsensitiveCollation, CollationRows = ["o1", "o3", "o6"] },
        new("nested_null", "ShipTo.Region = null", null),
        new("null_compare", "Note = null", null),
        new("not_null_compare", "Note != null", null),
        new("null_not_equal", "Note != @0", null, "gift") { Verdict = ShapeVerdict.PolicyRefuses },
        new("null_not_of_equal", "not (Note = @0)", null, "gift") { Verdict = ShapeVerdict.PolicyRefuses },
        new("null_not_contains", "not Note.Contains(@0)", null, "door") { Verdict = ShapeVerdict.PolicyRefuses, OracleWhere = "Note = null or not Note.Contains(@0)" },
        new("nullable_int_not_equal", "Discount != @0", null, 5) { Verdict = ShapeVerdict.PolicyRefuses },
        new("nested_null_not_equal", "BillTo.City != @0", null, "Austin") { Verdict = ShapeVerdict.PolicyRefuses },
        new("null_not_equal_guarded_or", "Note != @0 or Note = null", null, "gift"),
        new("null_not_equal_guarded_and", "Note != null and Note != @0", null, "gift"),
        new("nullable_int_not_equal_guarded", "Discount != @0 or Discount = null", null, 5),
        new("string_not_equal", "CustomerName != @0", null, "Gamma"),
        new("starts_with", "CustomerName.StartsWith(@0)", null, "Al") { Verdict = ShapeVerdict.CaseInsensitiveCollation, CollationRows = ["o1", "o3"] },
        new("contains", "CustomerName.Contains(@0)", null, "ta"),
        new("ends_with", "CustomerName.EndsWith(@0)", null, "a"),
        new("to_lower", "CustomerName.ToLower() = @0", null, "alpha co"),
        new("to_lower_starts_with", "CustomerName.ToLower().StartsWith(@0)", null, "alpha") { Verdict = ShapeVerdict.ProviderRefuses },
        new("bool_member", "Priority", null),
        new("bool_equals", "Priority = true", null),
        new("bool_not", "not Priority", null),
        new("guid_equals", "CustomerId = @0", null, Acme),
        new("guid_equals_text", "CustomerId = @0", null, Acme.ToString()),
        new("datetime_greater", "PlacedAt > @0", null, new DateTime(2025, 3, 15, 0, 0, 0, DateTimeKind.Utc)),
        new("offset_greater", "Created > @0", null, new DateTimeOffset(2025, 3, 15, 0, 0, 0, TimeSpan.Zero)),
        new("offset_greater_text", "Created > @0", null, "2025-03-15T00:00:00Z"),
        new("and_or_not", "(Total > @0 or Priority) and not (Status = \"Cancelled\")", null, 200m),
        new("string_length", "CustomerName.Length > @0", null, 5),
        new("dateonly_year", "DueDate.Year = @0", null, 2025) { Verdict = ShapeVerdict.PolicyRefuses },
        new("dateonly_greater", "DueDate > @0", null, new DateOnly(2025, 6, 1)),
        new("nullable_date_has_value", "ShippedAt.HasValue", null),
        new("nullable_date_null", "ShippedAt = null", null),
        new("nullable_date_value_year", "ShippedAt.Value.Year = @0", null, 2025) { Verdict = ShapeVerdict.PolicyRefuses, OracleWhere = "ShippedAt.HasValue and ShippedAt.Value.Year = @0" },
        new("nullable_date_value_greater", "ShippedAt.Value > @0", null, new DateTime(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc)) { Verdict = ShapeVerdict.PolicyRefuses, OracleWhere = "ShippedAt.HasValue and ShippedAt.Value > @0" },
        new("nullable_int_value_greater", "Rating.Value > @0", null, 4) { Verdict = ShapeVerdict.PolicyRefuses, OracleWhere = "Rating.HasValue and Rating.Value > @0" },
        new("nullable_int_has_value", "Rating.HasValue", null),
        new("nullable_int_greater", "Rating > @0", null, 4),
        new("timespan_member", "Duration.TotalMinutes > @0", null, 100d) { Verdict = ShapeVerdict.PolicyRefuses },
        new("timespan_greater", "Duration > @0", null, TimeSpan.FromMinutes(100)),
        new("timeonly_hour", "Window.Hour = @0", null, 10) { Verdict = ShapeVerdict.PolicyRefuses },
        new("timeonly_greater", "Window > @0", null, new TimeOnly(11, 0)),
        new("nullable_date_greater", "ShippedAt > @0", null, new DateTime(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc)),
        new("order_decimal", null, "Total desc"),
        new("order_int", null, "Quantity"),
        new("order_datetime", null, "PlacedAt desc"),
        new("order_offset", null, "Created"),
        new("order_dateonly", null, "DueDate desc"),
        new("order_enum", null, "Status desc") { VerdictWhenEnumsAreNames = ShapeVerdict.PolicyRefuses },
        new("order_nested", null, "ShipTo.City") { Verdict = ShapeVerdict.CaseInsensitiveCollation, CollationRows = ["o1", "o3", "o6", "o2", "o4", "o5", "o7", "o8"] },
        new("order_string", null, "CustomerName"),
        new("order_bool", null, "Priority desc"),
        new("order_by_id_explicitly", null, "Id desc"),
        new("order_then_by_id", null, "Quantity, Id desc")
    ];

    /// <summary>
    ///     LINQ to objects over the same text. The ids come back in the oracle's order when the shape has
    ///     an ordering, sorted by id otherwise.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026")]
    public static (IReadOnlyList<Guid>? ids, string? error) Oracle(IReadOnlyList<CriteriaOrder> docs, CriteriaShape shape)
    {
        try
        {
            // The store tie-breaks a given ordering by the identity; the oracle does the same so equal keys
            // compare deterministically.
            var orderBy = shape.OrderBy is null ? null : shape.OrderBy + ", Id";
            var query = DynamicQuery.Apply(docs.AsQueryable(),
                new DynamicQueryText(shape.OracleWhere ?? shape.Where, orderBy, shape.Args));
            var ids = query.Select(x => x.Id).ToList();
            return (shape.OrderBy is null ? ids.OrderBy(x => x).ToList() : ids, null);
        }
        catch (Exception e)
        {
            return (null, $"ORACLE THROWS {e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
    }

    public static async Task<DocumentStore> CreateStoreAsync(string schema, EnumStorage enumStorage,
        Casing casing = Casing.CamelCase)
    {
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"IF OBJECT_ID('[{schema}].[pc_doc_criteriaorder]','U') IS NOT NULL DROP TABLE [{schema}].[pc_doc_criteriaorder];";
            await cmd.ExecuteNonQueryAsync();
        }

        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.ConfigureSerialization(enumStorage, casing);
            opts.Schema.For<CriteriaOrder>();
        });
    }

    public static async Task<IReadOnlyList<CriteriaOrder>> SeedAsync(DocumentStore store)
    {
        var docs = Documents();
        await using var session = store.LightweightSession();
        foreach (var doc in docs) session.Store(doc);
        await session.SaveChangesAsync();
        return docs;
    }
}
