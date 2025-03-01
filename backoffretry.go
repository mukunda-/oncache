//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

// A simple backoff-retry calculator.
type backoffRetry struct {
	period float64
	limit  float64
	rate   float64
}

func (br *backoffRetry) reset(period float64) {
	br.period = period
}

func (br *backoffRetry) get() float64 {
	period := br.period
	br.period = br.period * br.rate
	if br.period > br.limit {
		br.period = br.limit
	}
	return period
}
