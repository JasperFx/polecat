namespace Polecat.EntityFrameworkCore.Tests;

public class RequiresNativeJsonFactAttribute : FactAttribute
{
    private static bool? cachedCondition;

    public RequiresNativeJsonFactAttribute()
    {
        cachedCondition ??= ConnectionSource.SupportsNativeJson;

        if (cachedCondition == false)
            Skip = "Database does not support native JSON, skipping the test";
    }
}
