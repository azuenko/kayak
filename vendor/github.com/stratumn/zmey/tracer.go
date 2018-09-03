package zmey

import (
	"fmt"
)

// Tracer organizes traces
type Tracer struct {
	prefix string
}

// NewTracer creates an instance of Tracer. Its arguments are similar to
// `fmt.Printf`, and format the prefix of traces.
func NewTracer(format string, a ...interface{}) Tracer {
	t := Tracer{
		prefix: fmt.Sprintf(format, a...),
	}
	return t
}

// Logf formats the trace and adds prefix (if defined)
func (t Tracer) Logf(format string, a ...interface{}) string {
	if t.prefix != "" {
		return t.prefix + ": " + fmt.Sprintf(format, a...)
	}
	return fmt.Sprintf(format, a...)
}

// Fork creates a new instance of Tracer with appended and formatted prefix
func (t Tracer) Fork(format string, a ...interface{}) Tracer {
	forkedTracer := Tracer{
		prefix: t.prefix + ": " + fmt.Sprintf(format, a...),
	}
	return forkedTracer
}

// Errorf is similar to fmt.Errorf, but uses tracer's prefix
func (t Tracer) Errorf(format string, a ...interface{}) error {
	if t.prefix != "" {
		return fmt.Errorf(t.prefix+": "+format, a...)
	}
	return fmt.Errorf(format, a...)
}
