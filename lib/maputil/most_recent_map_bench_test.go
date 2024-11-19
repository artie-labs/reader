package maputil

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkMostRecentMap(b *testing.B) {
	mre := NewMostRecentMap[string]()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Prepopulate the map with some items
	for i := 0; i < 1000; i++ {
		ts := rng.Int63n(1000)
		mre.AddItem(ts, "value")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ts := rng.Int63n(1000)
		if rng.Intn(2) == 0 {
			mre.AddItem(ts, "value")
		} else {
			mre.GetItem(ts)
		}
	}
}
