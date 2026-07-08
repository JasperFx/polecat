using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Weasel.SqlServer;

namespace Polecat.Tests.Storage;

/// <summary>
///     Polecat.Internal.IStorageOperation is converged onto the dialect-neutral
///     Weasel.Storage.IStorageOperation (#273): the shared two-arg ConfigureCommand bridges to
///     Polecat's one-arg SqlServer overload via a default interface method.
/// </summary>
public class storage_operation_seam_tests
{
    [Fact]
    public void polecat_operations_are_shared_storage_operations()
    {
        var operation = new ExecuteSqlStorageOperation("SELECT 1");

        operation.ShouldBeAssignableTo<Weasel.Storage.IStorageOperation>();
        operation.ShouldBeAssignableTo<Weasel.Core.IStorageOperation>();
    }

    [Fact]
    public void shared_configure_command_bridges_to_the_sqlserver_overload()
    {
        var operation = new ExecuteSqlStorageOperation("SELECT ?", 42);
        var seam = (Weasel.Storage.IStorageOperation)operation;

        // Same builder type Polecat's execution pipeline hands operations
        var builder = new BatchBuilder();

        // Session goes unused — Polecat operations are fully bound at construction
        seam.ConfigureCommand(builder, null!);

        var batch = builder.Compile();
        batch.BatchCommands.Count.ShouldBe(1);
        batch.BatchCommands[0].CommandText.ShouldContain("SELECT");
        batch.BatchCommands[0].Parameters.Count.ShouldBe(1);
        batch.BatchCommands[0].Parameters[0].Value.ShouldBe(42);
    }

    [Fact]
    public void shared_configure_command_rejects_non_sqlserver_builders()
    {
        var operation = new ExecuteSqlStorageOperation("SELECT 1");
        var seam = (Weasel.Storage.IStorageOperation)operation;

        var foreign = new NotASqlServerBuilder();
        Should.Throw<ArgumentException>(() => seam.ConfigureCommand(foreign, null!));
    }

    private sealed class NotASqlServerBuilder : Weasel.Core.ICommandBuilder
    {
        public string TenantId { get; set; } = string.Empty;
        public string? LastParameterName => null;
        public void Append(string sql) { }
        public void Append(char character) { }
        public void AppendParameters(params object[] parameters) { }
        // Weasel 9.16.0 (weasel#339): dialect-neutral grouped-parameter seam on Weasel.Core.ICommandBuilder.
        public System.Data.Common.DbParameter AppendParameter(object value) => throw new NotSupportedException();
        public Weasel.Core.IGroupedParameterBuilder CreateGroupedParameterBuilder(char? separator = null) => throw new NotSupportedException();
        public System.Data.Common.DbParameter[] AppendWithDbParameters(string text) => [];
        public System.Data.Common.DbParameter[] AppendWithDbParameters(string text, char placeholder) => [];
        public void StartNewCommand() { }
        public void AddParameters(object parameters) { }
        public void AddParameters(IDictionary<string, object?> parameters) { }
        public void AddParameters<T>(IDictionary<string, T> parameters) { }
    }
}
