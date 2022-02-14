package events

import (
	"math/rand"
	"time"
)

// Package variables to ensure random number generation
// is actually pseudo random
var source rand.Source
var random *rand.Rand

func init() {
	source = rand.NewSource(time.Now().UnixNano())
	random = rand.New(source)
}

// SetRandom allows the use of an externally created random number generator.
// Helpful in situations when the same set of random values should be generated each time.
func SetRandom(r *rand.Rand) {
	random = r
}
