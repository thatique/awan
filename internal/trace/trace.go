package trace

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/thatique/awan/verr"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// A Tracer supports OpenCensus tracing and latency metrics.
type Tracer struct {
	Package        string
	Provider       string
	LatencyMeasure *stats.Float64Measure
}

// ProviderName returns the name of the provider associated with the driver value.
// It is intended to be used to set Tracer.Provider.
// It actually returns the package path of the driver's type.
func ProviderName(driver interface{}) string {
	// Return the last component of the package path.
	if driver == nil {
		return ""
	}
	t := reflect.TypeOf(driver)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath()
}

// Context key for starting time of a method call.
type startTimeKey struct{}

// Start adds a span to the trace, and prepares for recording a latency measurement.
func (t *Tracer) Start(ctx context.Context, methodName string) context.Context {
	fullName := t.Package + "." + methodName
	ctx, _ = trace.StartSpan(ctx, fullName)
	ctx, err := tag.New(ctx,
		tag.Upsert(MethodKey, fullName),
		tag.Upsert(ProviderKey, t.Provider))
	if err != nil {
		// The only possible errors are from invalid key or value names, and those are programming
		// errors that will be found during testing.
		panic(fmt.Sprintf("fullName=%q, provider=%q: %v", fullName, t.Provider, err))
	}
	return context.WithValue(ctx, startTimeKey{}, time.Now())
}

// End ends a span with the given error, and records a latency measurement.
func (t *Tracer) End(ctx context.Context, err error) {
	startTime := ctx.Value(startTimeKey{}).(time.Time)
	elapsed := time.Since(startTime)
	code := verr.Code(err)
	span := trace.FromContext(ctx)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(code), Message: err.Error()})
	}
	span.End()
	stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(StatusKey, fmt.Sprint(code))},
		t.LatencyMeasure.M(float64(elapsed.Nanoseconds())/1e6)) // milliseconds
}
