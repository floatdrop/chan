package batch_test

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/chan/batch"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		in   []int
		size int
		want [][]int
	}{
		{"full batches", []int{1, 2, 3, 4, 5, 6}, 2, [][]int{{1, 2}, {3, 4}, {5, 6}}},
		{"short last batch", []int{1, 2, 3, 4, 5}, 2, [][]int{{1, 2}, {3, 4}, {5}}},
		{"size one", []int{1, 2, 3}, 1, [][]int{{1}, {2}, {3}}},
		{"size above input", []int{1, 2}, 5, [][]int{{1, 2}}},
		{"empty input", nil, 3, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collect(batch.New(t.Context(), source(tt.in...), tt.size))
			if !equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewFunc(t *testing.T) {
	gap := func(prev, next int) bool { return next-prev > 1 }
	tests := []struct {
		name string
		size int
		want [][]int
	}{
		{"split only", 10, [][]int{{1, 2, 3}, {5, 6}, {9}}},
		{"split and size", 2, [][]int{{1, 2}, {3}, {5, 6}, {9}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collect(batch.NewFunc(t.Context(), source(1, 2, 3, 5, 6, 9), tt.size, gap))
			if !equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeoutCountsFromFirstValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := make(chan int)
		out := batch.New(t.Context(), in, 10, batch.WithTimeout(100*time.Millisecond))

		time.Sleep(250 * time.Millisecond) // An idle batcher has no timeout running.
		start := time.Now()
		in <- 1
		time.Sleep(60 * time.Millisecond)
		in <- 2 // A later value does not extend the timeout.

		if got := <-out; !slices.Equal(got, []int{1, 2}) {
			t.Errorf("got %v, want [1 2]", got)
		}
		if elapsed := time.Since(start); elapsed != 100*time.Millisecond {
			t.Errorf("batch sent after %v, want 100ms", elapsed)
		}
		close(in)
		if b, ok := <-out; ok {
			t.Errorf("got %v after close, want closed channel", b)
		}
	})
}

func TestTimeoutRestartsAfterFullBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := make(chan int)
		out := batch.New(t.Context(), in, 2, batch.WithTimeout(100*time.Millisecond))

		in <- 1
		in <- 2
		if got := <-out; !slices.Equal(got, []int{1, 2}) {
			t.Errorf("got %v, want [1 2]", got)
		}
		time.Sleep(50 * time.Millisecond)
		start := time.Now()
		in <- 3
		if got := <-out; !slices.Equal(got, []int{3}) {
			t.Errorf("got %v, want [3]", got)
		}
		if elapsed := time.Since(start); elapsed != 100*time.Millisecond {
			t.Errorf("batch sent after %v, want 100ms", elapsed)
		}
		close(in)
	})
}

func TestTimeoutNeverSendsEmptyBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := make(chan int)
		out := batch.New(t.Context(), in, 10, batch.WithTimeout(10*time.Millisecond))
		time.Sleep(time.Second)
		close(in)
		if b, ok := <-out; ok {
			t.Errorf("got %v, want closed channel", b)
		}
	})
}

func TestCancelDiscardsUnfinishedBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		in := make(chan int)
		out := batch.New(ctx, in, 10)

		in <- 1
		cancel()
		if b, ok := <-out; ok {
			t.Errorf("got %v after cancel, want closed channel", b)
		}
	})
}

func TestCancelUnblocksSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		out := batch.New(ctx, source(1, 2, 3), 1)

		synctest.Wait() // The batcher is blocked: nobody receives [1].
		cancel()
		synctest.Wait() // It must give up the send and exit, or synctest reports a deadlock.
		if b, ok := <-out; ok {
			t.Errorf("got %v after cancel, want closed channel", b)
		}
	})
}

func TestCanceledSendsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got := collect(batch.New(ctx, source(make([]int, 100)...), 1)); len(got) != 0 {
		t.Errorf("got %d batches with a canceled context, want none", len(got))
	}
}

func TestWithBuffer(t *testing.T) {
	out := batch.New(t.Context(), source[int](), 1, batch.WithBuffer(4))
	if got := cap(out); got != 4 {
		t.Errorf("cap(out) = %d, want 4", got)
	}
}

func TestPanics(t *testing.T) {
	in := source[int]()
	tests := map[string]func(){
		"zero size":        func() { batch.New(t.Context(), in, 0) },
		"zero timeout":     func() { batch.WithTimeout(0) },
		"negative timeout": func() { batch.WithTimeout(-time.Second) },
		"negative buffer":  func() { batch.WithBuffer(-1) },
	}
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("did not panic")
				}
			}()
			f()
		})
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

func equal(a, b [][]int) bool {
	return slices.EqualFunc(a, b, slices.Equal[[]int])
}
