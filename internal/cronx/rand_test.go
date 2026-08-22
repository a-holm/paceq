package cronx

// tinyRand is a fixed seed splitmix64. The property tests need a case mix
// that is wide but identical on every run, so the standard library random
// package is deliberately not used: no seeding, no global state, no chance of
// a different run picking different windows.
type tinyRand struct {
	state uint64
}

func newRand(seed uint64) *tinyRand {
	return &tinyRand{state: seed}
}

func (r *tinyRand) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *tinyRand) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}
