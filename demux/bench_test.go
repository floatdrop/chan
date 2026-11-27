package demux_test

import (
	"fmt"
	"testing"

	"github.com/floatdrop/chan/demux"
)

// One op is one value routed. Keys are assigned round-robin, the worst case
// for locality.

func BenchmarkByKey(b *testing.B) {
	for _, keys := range []int{1, 16, 1024} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			bench(b, keys, demux.WithBuffer(64))
		})
	}
}

// BenchmarkByKeyChurn routes 1024 keys through a limit of 16 open keys,
// so nearly every value closes one consumer and starts another.
func BenchmarkByKeyChurn(b *testing.B) {
	bench(b, 1024, demux.WithMaxKeys(16), demux.WithBuffer(64))
}

func bench(b *testing.B, keys int, opts ...demux.Option) {
	b.ReportAllocs()
	in := make(chan int, 64)
	errc := make(chan error)
	key := func(v int) int { return v % keys }
	discard := func(_ int, ch <-chan int) {
		for range ch {
		}
	}
	go func() { errc <- demux.ByKey(b.Context(), in, key, discard, opts...) }()
	i := 0
	for b.Loop() {
		in <- i
		i++
	}
	close(in)
	if err := <-errc; err != nil {
		b.Fatal(err)
	}
}
