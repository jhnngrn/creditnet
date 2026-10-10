package core

import "math/bits"

// Demurrage rate is promille per year. 10 = 1%.
// decay = |balance| * rate * elapsed_s / (1000 * seconds_per_year)
const (
	rateBase     = 1000
	secsPerYear  = 365 * 24 * 3600 // 31_536_000
)

// mulDiv computes a * b / c with 128-bit intermediate to avoid overflow.
func mulDiv(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	if hi == 0 {
		return lo / c
	}
	if hi >= c {
		return ^uint64(0) // overflow — cap at max
	}
	q, _ := bits.Div64(hi, lo, c)
	return q
}

func demurrageDecay(absBalance uint64, rate uint64, elapsedSecs int64) uint64 {
	if rate == 0 || elapsedSecs <= 0 || absBalance == 0 {
		return 0
	}
	perYear := mulDiv(absBalance, rate, rateBase)
	decay := mulDiv(perYear, uint64(elapsedSecs), secsPerYear)
	if decay > absBalance {
		return absBalance
	}
	return decay
}

// MaterializeDemurrage updates Balance to reflect decay up to timestamp ts.
// Must be called with a shared timestamp (from sync) for consistency.
func (l *Link) MaterializeDemurrage(ts int64) {
	if l.DemurrageTime == 0 || l.Balance == 0 {
		l.DemurrageTime = ts
		return
	}
	elapsed := ts - l.DemurrageTime
	if elapsed <= 0 {
		return
	}
	var rate uint64
	if l.Balance > 0 {
		rate = l.Demurrage // peer owes me, my rate
	} else {
		rate = l.DemurrageIn // I owe peer, peer's rate
	}
	if rate == 0 {
		l.DemurrageTime = ts
		return
	}
	abs := l.Balance
	if abs < 0 {
		abs = -abs
	}
	decay := demurrageDecay(uint64(abs), rate, elapsed)
	if decay >= uint64(abs) {
		l.Balance = 0
	} else if l.Balance > 0 {
		l.Balance -= int64(decay)
	} else {
		l.Balance += int64(decay)
	}
	l.DemurrageTime = ts
}

// AddBalance materializes demurrage at ts (seconds), then applies delta.
func (l *Link) AddBalance(delta int64, ts int64) {
	l.MaterializeDemurrage(ts)
	l.Balance += delta
}

// EffectiveBalance returns balance after demurrage decay at the given time
// (seconds), without modifying the link. Used for bandwidth/pendingBalance reads.
func (l *Link) EffectiveBalance(nowSecs int64) int64 {
	if l.DemurrageTime == 0 || l.Balance == 0 {
		return l.Balance
	}
	elapsed := nowSecs - l.DemurrageTime
	if elapsed <= 0 {
		return l.Balance
	}
	var rate uint64
	if l.Balance > 0 {
		rate = l.Demurrage
	} else {
		rate = l.DemurrageIn
	}
	if rate == 0 {
		return l.Balance
	}
	abs := l.Balance
	if abs < 0 {
		abs = -abs
	}
	decay := demurrageDecay(uint64(abs), rate, elapsed)
	if decay >= uint64(abs) {
		return 0
	}
	if l.Balance > 0 {
		return l.Balance - int64(decay)
	}
	return l.Balance + int64(decay)
}
