// Package clock is the only source of time in the supervisor. Everything
// reads through Clock so tests run on fake time in milliseconds (spec §12).
package clock

import "time"

type Clock interface {
	Mono() float64 // monotonic seconds since process start; never goes backward
	Wall() float64 // wall-clock unix seconds; can step under NTP/suspend
	Sleep(seconds float64)
}

var (
	_ Clock = (*SystemClock)(nil)
	_ Clock = (*Fake)(nil)
)

type SystemClock struct{ start time.Time }

func NewSystem() *SystemClock { return &SystemClock{start: time.Now()} }

func (s *SystemClock) Mono() float64 { return time.Since(s.start).Seconds() }
func (s *SystemClock) Wall() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func (s *SystemClock) Sleep(sec float64) {
	time.Sleep(time.Duration(sec * float64(time.Second)))
}
