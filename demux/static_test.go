package demux_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/floatdrop/chan/demux"
)

func TestStatic(t *testing.T) {
	even, odd := make(chan int, 10), make(chan int, 10)
	parity := func(v int) string {
		switch {
		case v > 6:
			return "unknown"
		case v%2 == 0:
			return "even"
		default:
			return "odd"
		}
	}

	err := demux.Static(t.Context(), source(1, 2, 3, 4, 5, 6, 7, 8), parity, map[string]chan<- int{
		"even": even,
		"odd":  odd,
	})
	if err != nil {
		t.Fatalf("Static() = %v, want nil", err)
	}
	if got, want := collect(even), []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Errorf("even got %v, want %v", got, want)
	}
	if got, want := collect(odd), []int{1, 3, 5}; !slices.Equal(got, want) {
		t.Errorf("odd got %v, want %v", got, want)
	}
}

func TestStaticSharedChannel(t *testing.T) {
	out := make(chan int, 10)
	identity := func(v int) int { return v }

	err := demux.Static(t.Context(), source(1, 2, 3), identity, map[int]chan<- int{1: out, 2: out})
	if err != nil {
		t.Fatalf("Static() = %v, want nil", err)
	}
	if got, want := collect(out), []int{1, 2}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestStaticCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		in, out := make(chan int), make(chan int)
		errc := make(chan error)
		go func() {
			errc <- demux.Static(ctx, in, func(int) int { return 0 }, map[int]chan<- int{0: out})
		}()

		in <- 1
		synctest.Wait() // Static is blocked: nobody receives from out.
		cancel()
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("Static() = %v, want %v", err, context.Canceled)
		}
		if _, ok := <-out; ok {
			t.Error("out is open after Static returned")
		}
	})
}

func TestStaticCanceledRoutesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	out := make(chan int, 10)
	err := demux.Static(ctx, source(1, 2, 3), func(int) int { return 0 }, map[int]chan<- int{0: out})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Static() = %v, want %v", err, context.Canceled)
	}
	if got := collect(out); len(got) != 0 {
		t.Errorf("got %v with a canceled context, want nothing", got)
	}
}

// source returns a closed channel that holds values.
func source[T any](values ...T) <-chan T {
	ch := make(chan T, len(values))
	for _, v := range values {
		ch <- v
	}
	close(ch)
	return ch
}

func collect[T any](ch <-chan T) []T {
	var all []T
	for v := range ch {
		all = append(all, v)
	}
	return all
}
