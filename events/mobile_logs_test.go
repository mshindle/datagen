package events

import "testing"

func BenchmarkMockMobileLog(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = MockMobileLog()
	}
}
