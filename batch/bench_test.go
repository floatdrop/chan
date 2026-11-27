package batch_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/floatdrop/chan/batch"
)

// One op is one value sent into the batcher; allocs/op shrink as batches grow,
// since there is one allocation per batch.

func BenchmarkNew(b *testing.B) {
	for _, size := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			bench(b, func(in <-chan int) <-chan []int {
				return batch.New(b.Context(), in, size)
			})
		})
	}
}

func BenchmarkNewTimeout(b *testing.B) {
	for _, size := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			bench(b, func(in <-chan int) <-chan []int {
				return batch.New(b.Context(), in, size, batch.WithTimeout(time.Second))
			})
		})
	}
}

func BenchmarkNewFunc(b *testing.B) {
	// Every 16th value starts a new batch.
	split := func(_, next int) bool { return next%16 == 0 }
	bench(b, func(in <-chan int) <-chan []int {
		return batch.NewFunc(b.Context(), in, 256, split)
	})
}

func bench(b *testing.B, start func(<-chan int) <-chan []int) {
	b.ReportAllocs()
	in := make(chan int, 64)
	out := start(in)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range out {
		}
	}()
	i := 0
	for b.Loop() {
		in <- i
		i++
	}
	close(in)
	<-done
}
