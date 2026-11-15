using System.Reflection;
using System.Runtime.CompilerServices;

namespace Polecat.Tests.Compilation;

/// <summary>
///     #970, Critter Stack plan decision D4: runtime-async is on by default for the net11.0 build of Polecat's
///     own assemblies (PolecatRuntimeAsync in Directory.Build.props). net10.0 cannot load runtime-async code, so
///     it stays off there. Polecat has no runtime code generation, so there is no GenerationRules switch to pin.
///     Wolverine's CoreTests.Compilation.runtime_async_defaults_5150 is the model.
/// </summary>
/// <remarks>
///     A build with -p:PolecatRuntimeAsync=false fails the net11.0 branch on purpose: that is the opt-out, and
///     this test pins the default.
/// </remarks>
public class runtime_async_defaults_970
{
    // MethodImplAttributes.Async, the flag the compiler sets on a runtime-async method
    private const MethodImplAttributes AsyncFlag = (MethodImplAttributes)0x2000;

    [Fact]
    public void the_polecat_assembly_is_compiled_with_runtime_async_only_on_net11()
    {
        var types = typeof(DocumentStore).Assembly.GetTypes();

        var runtimeAsyncMethods = types
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                          BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Count(m => (m.MethodImplementationFlags & AsyncFlag) != 0);

        var stateMachines = types
            .Where(t => typeof(IAsyncStateMachine).IsAssignableFrom(t) &&
                        t.IsDefined(typeof(CompilerGeneratedAttribute)))
            .ToArray();

#if NET11_0_OR_GREATER
        runtimeAsyncMethods.ShouldBeGreaterThan(0);

        // Async iterators are not converted by runtime-async, so they keep their state machines. Every one left
        // must be an iterator's: an ordinary async method's state machine means the feature did not apply to it.
        stateMachines
            .Where(t => !t.GetInterfaces().Any(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>)))
            .Select(t => t.FullName)
            .ShouldBeEmpty();
#else
        runtimeAsyncMethods.ShouldBe(0);
        stateMachines.Length.ShouldBeGreaterThan(100);
#endif
    }
}
