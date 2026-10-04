using System.Reflection;
using System.Runtime.CompilerServices;
using JasperFx.Core.Reflection;
using Polecat.Storage;
using Polecat.Tests.StrongTypedId;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Aot;

/// <summary>
///     #733: the Native AOT fallbacks in <see cref="DocumentMapping" /> produce the same answers as
///     the fast paths they stand in for.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>These facts exist because the fallbacks are unreachable in this suite.</b> Both sit
///         behind <c>!RuntimeFeature.IsDynamicCodeSupported</c>, which is never true under the JIT, so
///         a full green run says nothing whatsoever about them — they could be empty and the suite
///         would not notice. The code they replace is reached instead, on every single test.
///     </para>
///     <para>
///         <b>What is asserted is AGREEMENT, not behaviour</b>, and that is the specific claim worth
///         holding. The argument for branching on the runtime feature at all was that the fast path
///         exists only to avoid per-call reflection: <c>LambdaBuilder</c> compiles a delegate,
///         <see cref="PropertyInfo.GetValue" /> does the same read slowly, and there is therefore no
///         second behaviour to keep in step. If that ever stops being true, these go red — which is
///         the only place it could be caught, since the AOT lane cannot currently get far enough to
///         exercise them (it stops in <c>CreateAssigner</c>; see weasel#689).
///     </para>
/// </remarks>
public class aot_fallback_agreement_tests
{
    private class PlainDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class IntDoc
    {
        public int Id { get; set; }
    }

    private class StringDoc
    {
        public string Id { get; set; } = string.Empty;
    }

    private static (Func<object, object?> getter, Action<object, object> setter) FastPath(PropertyInfo property)
    {
        // The generic route, reached by the same MakeGenericMethod the AOT path cannot use.
        var closed = typeof(DocumentMapping)
            .GetMethod("RawIdAccessors", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(property.DeclaringType!, property.PropertyType);
        return ((Func<object, object?>, Action<object, object>))closed.Invoke(null, [property])!;
    }

    [Fact]
    public void the_suite_runs_under_the_jit_so_the_fallbacks_are_otherwise_dead_code()
    {
        // Stated as a fact rather than a comment, because it is the premise of every assertion below
        // and it would be silently false on a natively-published test host.
        RuntimeFeature.IsDynamicCodeSupported.ShouldBeTrue();
    }

    [Theory]
    [InlineData(typeof(PlainDoc), "Id")]
    [InlineData(typeof(PlainDoc), "Name")]
    [InlineData(typeof(IntDoc), "Id")]
    [InlineData(typeof(StringDoc), "Id")]
    public void the_id_accessor_fallback_reads_what_the_fast_path_reads(Type documentType, string member)
    {
        var property = documentType.GetProperty(member)!;
        var document = Activator.CreateInstance(documentType)!;

        var fast = FastPath(property);
        var fallback = DocumentMapping.RawIdAccessorsByReflection(property);

        // Default value first -- a getter that returned null for an unset Guid instead of Guid.Empty
        // would be a difference the id-generation path reads as "already assigned".
        fallback.getter(document).ShouldBe(fast.getter(document));

        object value = member == "Name" || documentType == typeof(StringDoc)
            ? "written"
            : documentType == typeof(IntDoc) ? 42 : Guid.NewGuid();

        fallback.setter(document, value);
        fallback.getter(document).ShouldBe(value);
        fast.getter(document).ShouldBe(value);

        // ...and the fast path's setter is visible to the fallback's getter, which is what makes them
        // interchangeable rather than merely individually correct.
        var second = Activator.CreateInstance(documentType)!;
        fast.setter(second, value);
        fallback.getter(second).ShouldBe(value);
    }

    [Theory]
    [InlineData(typeof(InvoiceId))]
    [InlineData(typeof(OrderItemId))]
    [InlineData(typeof(BadgeId))]
    public void the_strong_typed_id_fallback_wraps_what_the_fast_path_wraps(Type wrapperType)
    {
        // ⚠️ The fallback composes ValueTypeInfo.ValueProperty / Ctor / Builder, while the fast path
        // goes through CreateWrapper / UnWrapper -- which jasperfx#942 taught to fall back to those
        // same members. So this pins that Polecat's spelling of that fallback agrees with JasperFx's,
        // which is the thing #736 assumed and could not check from here.
        var valueType = ValueTypeInfo.ForType(wrapperType);

        var closed = typeof(DocumentMapping)
            .GetMethod("StrongTypedIdConverters", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(valueType.OuterType, valueType.SimpleType);
        var fast = ((Func<object, object>, Func<object, object>))closed.Invoke(null, [valueType])!;

        var fallback = DocumentMapping.StrongTypedIdConvertersByReflection(valueType);

        object inner = valueType.SimpleType == typeof(Guid) ? Guid.NewGuid()
            : valueType.SimpleType == typeof(int) ? 7
            : "abc";

        var wrappedByFallback = fallback.wrapper(inner);
        var wrappedByFast = fast.Item2(inner);

        wrappedByFallback.ShouldBe(wrappedByFast);
        fallback.unwrapper(wrappedByFallback).ShouldBe(inner);

        // Cross-wise, so a pair that happened to be self-consistent but mutually wrong fails.
        fallback.unwrapper(wrappedByFast).ShouldBe(inner);
        fast.Item1(wrappedByFallback).ShouldBe(inner);
    }
}
