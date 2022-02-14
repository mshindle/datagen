package events

import (
	"fmt"
	"testing"
)

var dieTests = []struct {
	name  string
	sided int
	amt   int
}{
	{name: "one d6", sided: 6, amt: 1},
	{name: "two d6", sided: 6, amt: 2},
	{name: "three d6", sided: 6, amt: 3},
}

func TestCup_Throw(t *testing.T) {
	for _, tt := range dieTests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCup(tt.sided, tt.amt)
			got := c.Throw()
			if len(got.Rolls) != tt.amt {
				t.Errorf("number of dice wanted = %v, got %v", tt.amt, got)
			}
			var total int
			for _, val := range got.Rolls {
				if val < 1 || val > tt.sided {
					t.Errorf("value should be in range 1 to %d, got %d", tt.sided, val)
				}
				total += val
			}
			if total != got.Total {
				t.Errorf("total of rolls should be %d, got %d", total, got.Total)
			}
		})
	}
}

func BenchmarkCup_Throw(b *testing.B) {
	for _, d := range dieTests {
		c := NewCup(d.sided, d.amt)
		b.Run(fmt.Sprintf("%dd%d", d.amt, d.sided), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = c.Throw()
			}
		})
	}
}
