package ring_test

import (
	"fmt"
	"testing"

	"github.com/floatdrop/chan/ring"
)

// One op is one value sent.

func BenchmarkSend(b *testing.B) {
	for _, size := range []int{1, 64} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			r := ring.New[int](size)
			done := drain(r.C)
			for b.Loop() {
				r.Send(1)
			}
			r.Close()
			<-done
		})
	}
}

// BenchmarkSendFull measures Send with no receiver: every call discards a value.
func BenchmarkSendFull(b *testing.B) {
	r := ring.New[int](64)
	for b.Loop() {
		r.Send(1)
	}
}

func BenchmarkSendParallel(b *testing.B) {
	r := ring.New[int](64)
	done := drain(r.C)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Send(1)
		}
	})
	r.Close()
	<-done
}

func BenchmarkFrom(b *testing.B) {
	in := make(chan int, 64)
	r := ring.From(b.Context(), in, 64)
	done := drain(r.C)
	for b.Loop() {
		in <- 1
	}
	close(in)
	<-done
}

// BenchmarkChan is the baseline: a plain buffered channel, which blocks
// instead of dropping values.
func BenchmarkChan(b *testing.B) {
	ch := make(chan int, 64)
	done := drain(ch)
	for b.Loop() {
		ch <- 1
	}
	close(ch)
	<-done
}

func drain[T any](ch <-chan T) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	return done
}
