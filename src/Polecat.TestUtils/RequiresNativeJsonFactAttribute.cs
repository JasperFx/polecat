using Xunit;

namespace Polecat.TestUtils;

public class RequiresNativeJsonFactAttribute : FactAttribute
{
    private static bool? cachedCondition;

    public RequiresNativeJsonFactAttribute(bool value)
    {
        cachedCondition ??= ConnectionSource.SupportsNativeJson;

        if (value && cachedCondition == false)
            Skip = "Used SQL Server version must support native JSON, skipping the test";

        if (!value && cachedCondition == true)
            Skip = "Used SQL Server version must NOT support native JSON, skipping the test";
    }
}
