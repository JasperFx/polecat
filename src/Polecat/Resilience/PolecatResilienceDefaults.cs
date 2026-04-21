using Polly;

namespace Polecat.Resilience;

internal static class PolecatResilienceDefaults
{
    public static ResiliencePipelineBuilder AddPolecatDefaults(this ResiliencePipelineBuilder builder)
    {
        return builder;
    }
}
