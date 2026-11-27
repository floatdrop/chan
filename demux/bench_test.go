package demux_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/floatdrop/chan/demux"
)

// One op is one value routed. Keys are assigned round-robin, the worst case
// for locality.

func BenchmarkStatic(b *testing.B) {
	for _, keys := range []int{1, 16} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			var wg sync.WaitGroup
			outs := make(map[int]chan<- int, keys)
			for k := range keys {
				ch := make(chan int, 64)
				outs[k] = ch
				wg.Go(func() {
					for range ch {
					}
				})
			}
			bench(b, func(in <-chan int, key func(int) int) error {
				return demux.Static(b.Context(), in, key, outs)
			}, keys)
			wg.Wait()
		})
	}
}

func BenchmarkDynamic(b *testing.B) {
	for _, keys := range []int{1, 16, 1024} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			bench(b, func(in <-chan int, key func(int) int) error {
				return demux.Dynamic(b.Context(), in, key, discard, demux.WithBuffer(64))
			}, keys)
		})
	}
}

// BenchmarkDynamicChurn routes 1024 keys through a limit of 16 open keys,
// so nearly every value closes one consumer and starts another.
func BenchmarkDynamicChurn(b *testing.B) {
	bench(b, func(in <-chan int, key func(int) int) error {
		return demux.Dynamic(b.Context(), in, key, discard, demux.WithMaxKeys(16), demux.WithBuffer(64))
	}, 1024)
}

func discard(_ int, ch <-chan int) {
	for range ch {
	}
}

func bench(b *testing.B, run func(<-chan int, func(int) int) error, keys int) {
	b.ReportAllocs()
	in := make(chan int, 64)
	errc := make(chan error)
	go func() { errc <- run(in, func(v int) int { return v % keys }) }()
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
