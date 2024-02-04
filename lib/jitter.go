package lib

import (
	"math/rand"
)

func JitterMs(baseMs, maxMs, attempts int) int {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	// 2 ** x == 1 << x
	return rand.Intn(min(maxMs, baseMs*(1<<attempts)))
}
