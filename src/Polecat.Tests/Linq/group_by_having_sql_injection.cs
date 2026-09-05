using Polecat.Linq.Parsing;
using Shouldly;
using Weasel.SqlServer;

namespace Polecat.Tests.Linq;

// Security regression for the LINQ/patching SQL-injection audit
// (Stoat plan critter-hardening-audit, node polecat-sqli-audit).
//
// A HAVING clause built from Where(...) after GroupBy(...) compares an aggregate against a runtime
// constant, e.g. .Where(g => g.Max(x => x.Color) == someValue). Before the fix the constant operand
// was rendered into the SQL as constant.Value.ToString() with no escaping or parameterization, so a
// string-typed operand could break out of the HAVING grammar and inject SQL — the same class as
// JasperFx/marten#4954 (non-string projection constants rendered unparameterized). The constant is
// now bound as a command parameter.
public class group_by_having_sql_injection
{
    private static SqlCommandRender Render(HavingComparisonFragment fragment)
    {
        var builder = new BatchBuilder();
        fragment.Apply(builder);
        var cmd = builder.Compile().BatchCommands[0];
        return new SqlCommandRender(cmd.CommandText,
            cmd.Parameters.Cast<Microsoft.Data.SqlClient.SqlParameter>().Select(p => p.Value).ToArray());
    }

    private sealed record SqlCommandRender(string CommandText, object?[] ParameterValues);

    [Fact]
    public void string_having_constant_is_bound_as_a_parameter_not_injected()
    {
        const string attack = "x' OR '1'='1"; // classic breakout attempt

        var fragment = new HavingComparisonFragment(
            HavingOperand.Aggregate("MAX(JSON_VALUE(data, '$.color'))"),
            "=",
            HavingOperand.Constant(attack));

        var render = Render(fragment);

        // The attacker text must be absent from the SQL grammar and present in a parameter.
        render.CommandText.ShouldNotContain(attack);
        render.CommandText.ShouldNotContain("'1'='1");
        render.ParameterValues.ShouldContain(attack);
    }

    [Fact]
    public void numeric_having_constant_is_bound_as_a_parameter()
    {
        var fragment = new HavingComparisonFragment(
            HavingOperand.Aggregate("COUNT(*)"),
            ">",
            HavingOperand.Constant(5));

        var render = Render(fragment);

        render.CommandText.ShouldContain("COUNT(*) > @");
        render.ParameterValues.ShouldContain(o => Equals(o, 5));
    }

    [Fact]
    public void aggregate_operands_are_still_rendered_inline()
    {
        var fragment = new HavingComparisonFragment(
            HavingOperand.Aggregate("MIN(JSON_VALUE(data, '$.age'))"),
            "<",
            HavingOperand.Aggregate("MAX(JSON_VALUE(data, '$.age'))"));

        var render = Render(fragment);

        render.CommandText.ShouldBe(
            "MIN(JSON_VALUE(data, '$.age')) < MAX(JSON_VALUE(data, '$.age'))");
        render.ParameterValues.ShouldBeEmpty();
    }
}
