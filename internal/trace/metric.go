package trace

import (
	"fmt"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// LatencyMeasure returns the measure for method call latency
func LatencyMeasure(pkg string) *stats.Float64Measure {
	return stats.Float64(
		pkg+"/latency",
		"Latency of method call",
		stats.UnitMilliseconds)
}

// Tag keys used for the standard views.
var (
	MethodKey   = mustKey("method")
	StatusKey   = mustKey("status")
	ProviderKey = mustKey("provider")
)

// The following variables define the default hard-coded auxiliary data used by
// both the default GRPC client and GRPC server metrics.
var (
	DefaultBytesDistribution        = view.Distribution(1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296)
	DefaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)
	DefaultMessageCountDistribution = view.Distribution(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536)
)

func mustKey(name string) tag.Key {
	k, err := tag.NewKey(name)
	if err != nil {
		panic(fmt.Sprintf("tag.NewKey(%q): %v", name, err))
	}
	return k
}

// Views returns the views supported by Go CDK APIs.
func Views(pkg string, latencyMeasure *stats.Float64Measure) []*view.View {
	return []*view.View{
		{
			Name:        pkg + "/completed_calls",
			Measure:     latencyMeasure,
			Description: "Count of method calls by provider, method and status.",
			TagKeys:     []tag.Key{ProviderKey, MethodKey, StatusKey},
			Aggregation: view.Count(),
		},
		{
			Name:        pkg + "/latency",
			Measure:     latencyMeasure,
			Description: "Distribution of method latency, by provider and method.",
			TagKeys:     []tag.Key{ProviderKey, MethodKey},
			Aggregation: DefaultMillisecondsDistribution,
		},
	}
}
