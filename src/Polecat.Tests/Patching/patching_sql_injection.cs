using Polecat.Patching;
using Shouldly;
using Weasel.SqlServer;

namespace Polecat.Tests.Patching;

// Security regression for the LINQ/patching SQL-injection audit
// (Stoat plan critter-hardening-audit, node polecat-sqli-audit).
//
// A JSON path segment in a patch is assembled from runtime-supplied strings — a dictionary indexer
// key (JsonPathHelper), or a property/key name passed straight to the patch API such as
// Patch.Set("name", ...), Append(expr, key, ...), Remove(expr, key). Each segment is inlined into a
// single-quoted '$.{path}' literal inside a JSON_MODIFY call. Before the fix a value containing a
// single quote terminated that literal and injected arbitrary SQL into the generated UPDATE — the
// same class of hole as JasperFx/marten#4911 (DictionaryItemMember JSON-path locator).
//
// These are pure command-rendering assertions (no database): they render each PatchOperation sink
// against a real SQL Server CommandBuilder and assert embedded single quotes are doubled, so the
// attacker text stays inside the literal as data.
public class patching_sql_injection
{
    // A key that, unescaped, breaks out of the '$....' literal and appends its own SQL.
    private const string MaliciousKey = "evil' OR '1'='1'; DROP TABLE pc_doc_target; --";
    private const string EscapedKey = "evil'' OR ''1''=''1''; DROP TABLE pc_doc_target; --";

    private static string Render(Action<ICommandBuilder> action)
    {
        var builder = new BatchBuilder();
        action(builder);
        return builder.Compile().BatchCommands[0].CommandText;
    }

    [Fact]
    public void set_scalar_escapes_single_quotes_in_the_path()
    {
        var sql = Render(PatchOperation.SetScalar(MaliciousKey, "value"));

        sql.ShouldContain($"'$.{EscapedKey}'");
        sql.ShouldNotContain($"'$.{MaliciousKey}'");
    }

    [Fact]
    public void delete_property_escapes_single_quotes_in_the_path()
    {
        var sql = Render(PatchOperation.DeleteProperty(MaliciousKey));

        sql.ShouldContain(EscapedKey);
        sql.ShouldNotContain($"$.{MaliciousKey}',");
    }

    [Fact]
    public void set_dict_key_escapes_both_the_dictionary_path_and_the_runtime_key()
    {
        // Append(expr, key, element) / AppendIfNotExists route through SetDictKey with the caller's
        // raw dictionary key. The dictionary path AND the key must both be escaped.
        var sql = Render(PatchOperation.SetDictKey("attributes", MaliciousKey, "\"v\""));

        sql.ShouldContain($"'$.attributes.{EscapedKey}'");
        sql.ShouldNotContain($"'$.attributes.{MaliciousKey}'");
    }

    [Fact]
    public void remove_dict_key_escapes_the_runtime_key()
    {
        var sql = Render(PatchOperation.RemoveDictKey("attributes", MaliciousKey));

        sql.ShouldContain($"'$.attributes.{EscapedKey}'");
        sql.ShouldNotContain($"'$.attributes.{MaliciousKey}'");
    }

    [Fact]
    public void increment_escapes_single_quotes_in_the_path()
    {
        var sql = Render(PatchOperation.IncrementInt(MaliciousKey, 1, "int"));

        sql.ShouldContain(EscapedKey);
        // The bare, odd-quoted breakout sequence must not survive anywhere in the command.
        sql.ShouldNotContain($"$.{MaliciousKey}'");
    }

    [Fact]
    public void append_scalar_escapes_single_quotes_in_the_path()
    {
        var sql = Render(PatchOperation.AppendScalar(MaliciousKey, "value"));

        sql.ShouldContain($"append $.{EscapedKey}");
        sql.ShouldNotContain($"append $.{MaliciousKey}'");
    }

    [Fact]
    public void rename_property_escapes_both_paths()
    {
        var sql = Render(PatchOperation.RenameProperty(MaliciousKey, "newName", isScalarType: true));

        sql.ShouldContain(EscapedKey);
        sql.ShouldNotContain($"$.{MaliciousKey}'");
    }

    [Fact]
    public void duplicate_property_escapes_source_and_destination_paths()
    {
        var sql = Render(PatchOperation.DuplicateProperty(
            MaliciousKey, new[] { MaliciousKey }, isScalarType: true));

        sql.ShouldContain(EscapedKey);
        sql.ShouldNotContain($"$.{MaliciousKey}'");
    }

    // A legitimate key that happens to contain an apostrophe now round-trips as data rather than
    // erroring or injecting — the escaping is correct, not merely defensive.
    [Fact]
    public void a_legitimate_key_with_an_apostrophe_is_preserved_as_data()
    {
        var sql = Render(PatchOperation.SetScalar("O'Brien", "value"));

        sql.ShouldContain("'$.O''Brien'");
    }
}
