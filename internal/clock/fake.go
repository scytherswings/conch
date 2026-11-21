package clock

// Fake advances only when told. AdvanceWallOnly lets suspend-detection tests
// diverge wall time from monotonic time deliberately.
type Fake struct {
	mono, wall float64
	Slept      []float64
}

func NewFake() *Fake { return &Fake{wall: 1_700_000_000} }

func (f *Fake) Mono() float64 { return f.mono }
func (f *Fake) Wall() float64 { return f.wall }
func (f *Fake) Sleep(sec float64) {
	f.Slept = append(f.Slept, sec)
	f.Advance(sec)
}
func (f *Fake) Advance(sec float64)         { f.mono += sec; f.wall += sec }
func (f *Fake) AdvanceWallOnly(sec float64) { f.wall += sec } // a suspend: wall moves, mono does not
