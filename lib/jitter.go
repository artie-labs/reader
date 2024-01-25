package lib

import (
	"math"
	"math/rand"
)

func JitterMs(baseMs, maxMs, attempts int) int {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	return rand.Intn(int(math.Min(float64(maxMs), float64(baseMs)*math.Pow(2, float64(attempts)))))
}
