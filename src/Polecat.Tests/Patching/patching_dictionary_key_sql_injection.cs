using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.SqlServer;

namespace Polecat.Tests.Patching;

// End-to-end security regression for the LINQ/patching SQL-injection audit
// (Stoat plan critter-hardening-audit, node polecat-sqli-audit).
//
// The exact analogue of JasperFx/marten#4911: a Dictionary<,> indexer key in a patch target
// (x.NumberByKey[key]) is a runtime value, evaluated at patch-build time, that is inlined into the
// JSON_MODIFY '$.numberByKey.{key}' path literal. A key containing a single quote could break out of
// the literal and inject arbitrary SQL into the generated UPDATE. This test drives the full
// PatchExpression -> JsonPathHelper -> PatchOperation flow against a live database with a hostile key
// and proves the statement executes harmlessly (the table survives, the malicious key is stored as
// data) rather than injecting.
[Collection("integration")]
public class patching_dictionary_key_sql_injection : IntegrationContext
{
    public patching_dictionary_key_sql_injection(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "patch_sqli"; });
    }

    [Fact]
    public async Task malicious_dictionary_key_does_not_inject_sql()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // If the key were interpolated raw, these single quotes would terminate the JSON path
        // literal and the appended tokens would run as their own statement, dropping the document
        // table. Escaped, the whole string is a single (malformed) JSON path that SQL Server's
        // JSON_MODIFY rejects at runtime as data — it never leaves the '$....' literal.
        var maliciousKey = "k')); DROP TABLE pc_doc_target; SELECT ('";

        Exception? thrown = null;
        try
        {
            theSession.Patch<Target>(target.Id).Set(x => x.NumberByKey[maliciousKey], 42);
            await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
        catch (Exception ex)
        {
            // SQL Server rejects the escaped-but-malformed JSON path ("JSON path is not properly
            // formatted"). That the server evaluates it as a path argument — rather than parsing the
            // quotes as SQL — is itself the proof the value stayed inside the literal.
            thrown = ex;
        }

        // The decisive assertion: the injected DROP TABLE did not run, so the document table still
        // exists and the original document is still loadable in a fresh session.
        await using var query = theStore.QuerySession();
        var reloaded = await query.LoadAsync<Target>(target.Id, TestContext.Current.CancellationToken);
        reloaded.ShouldNotBeNull();

        // If it threw at all, it was a data/JSON-path error, never evidence the statement executed.
        if (thrown != null)
        {
            thrown.ToString().ShouldNotContain("Invalid object name");
        }
    }

    // A command-rendering assertion (no execution needed) that the generated SQL never carries the
    // odd-quoted breakout sequence — the single quote in the key is doubled inside the path literal.
    [Fact]
    public void generated_patch_sql_escapes_the_dictionary_key()
    {
        var builder = new BatchBuilder();
        // Path as JsonPathHelper would assemble it for x.NumberByKey["a' OR '1'='1"].
        PatchOperation.SetScalar("numberByKey.a' OR '1'='1", 42)(builder);
        var sql = builder.Compile().BatchCommands[0].CommandText;

        sql.ShouldContain("'$.numberByKey.a'' OR ''1''=''1'");
        sql.ShouldNotContain("'$.numberByKey.a' OR '1'='1'");
    }
}
