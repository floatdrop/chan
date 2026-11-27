package demux_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/chan/demux"
)

type msg struct {
	key string
	n   int
}

func msgKey(m msg) string { return m.key }

// recorder is a consume function that records, per key, the values each of
// its calls received.
type recorder struct {
	mu    sync.Mutex
	calls map[string][][]int
}

func (r *recorder) consume(key string, ch <-chan msg) {
	var got []int
	for m := range ch {
		got = append(got, m.n)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls == nil {
		r.calls = make(map[string][][]int)
	}
	r.calls[key] = append(r.calls[key], got)
}

func (r *recorder) check(t *testing.T, want map[string][][]int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !maps.EqualFunc(r.calls, want, func(a, b [][]int) bool {
		return slices.EqualFunc(a, b, slices.Equal[[]int])
	}) {
		t.Errorf("consume calls got %v, want %v", r.calls, want)
	}
}

func TestByKey(t *testing.T) {
	in := source(msg{"a", 1}, msg{"b", 1}, msg{"a", 2}, msg{"c", 1}, msg{"b", 2}, msg{"a", 3})
	var rec recorder

	if err := demux.ByKey(t.Context(), in, msgKey, rec.consume); err != nil {
		t.Fatalf("ByKey() = %v, want nil", err)
	}
	rec.check(t, map[string][][]int{
		"a": {{1, 2, 3}},
		"b": {{1, 2}},
		"c": {{1}},
	})
}

func TestByKeyEmptyInput(t *testing.T) {
	var rec recorder
	if err := demux.ByKey(t.Context(), source[msg](), msgKey, rec.consume); err != nil {
		t.Fatalf("ByKey() = %v, want nil", err)
	}
	rec.check(t, nil)
}

func TestByKeyMaxKeysClosesLeastRecentlyUsed(t *testing.T) {
	// "a" is used after "b", so "b" is closed when "c" arrives;
	// then "a" is the least recently used when "b" comes back.
	in := source(msg{"a", 1}, msg{"b", 1}, msg{"a", 2}, msg{"c", 1}, msg{"b", 2})
	var rec recorder

	if err := demux.ByKey(t.Context(), in, msgKey, rec.consume, demux.WithMaxKeys(2)); err != nil {
		t.Fatalf("ByKey() = %v, want nil", err)
	}
	rec.check(t, map[string][][]int{
		"a": {{1, 2}},
		"b": {{1}, {2}},
		"c": {{1}},
	})
}

func TestByKeyReopenedKeyWaitsForPreviousConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			mu      sync.Mutex
			running = make(map[string]int)
		)
		slow := func(key string, ch <-chan string) {
			mu.Lock()
			running[key]++
			if running[key] > 1 {
				t.Errorf("%d consumers of %q run at once", running[key], key)
			}
			mu.Unlock()

			for range ch {
			}
			time.Sleep(time.Second) // Still busy after its channel is closed.

			mu.Lock()
			running[key]--
			mu.Unlock()
		}

		start := time.Now()
		identity := func(s string) string { return s }
		err := demux.ByKey(t.Context(), source("a", "b", "a"), identity, slow, demux.WithMaxKeys(1))
		if err != nil {
			t.Fatalf("ByKey() = %v, want nil", err)
		}
		// The two consumers of "a" ran one after the other.
		if elapsed := time.Since(start); elapsed != 2*time.Second {
			t.Errorf("ByKey returned after %v, want 2s", elapsed)
		}
	})
}

func TestByKeyCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		in := make(chan int)
		release := make(chan struct{})
		stuck := func(_ int, ch <-chan int) {
			<-release
			for range ch {
			}
		}
		errc := make(chan error)
		go func() {
			errc <- demux.ByKey(ctx, in, func(v int) int { return v }, stuck)
		}()

		in <- 1
		synctest.Wait() // ByKey is blocked: the consumer does not receive.
		cancel()
		synctest.Wait()
		select {
		case err := <-errc:
			t.Fatalf("ByKey() = %v before its consumer returned", err)
		default:
		}

		close(release)
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("ByKey() = %v, want %v", err, context.Canceled)
		}
	})
}

func TestByKeyConsumerReturnsEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The consumer of "a" gives up after one value; "b" must still get all of its values.
		in := source(msg{"a", 1}, msg{"a", 2}, msg{"b", 1}, msg{"a", 3}, msg{"b", 2})
		var rec recorder
		consume := func(key string, ch <-chan msg) {
			if key == "a" {
				m := <-ch
				rec.consume(key, source(m))
				return
			}
			rec.consume(key, ch)
		}

		if err := demux.ByKey(t.Context(), in, msgKey, consume); err != nil {
			t.Fatalf("ByKey() = %v, want nil", err)
		}
		rec.check(t, map[string][][]int{
			"a": {{1}},
			"b": {{1, 2}},
		})
	})
}

func TestByKeyWithBuffer(t *testing.T) {
	var got int
	err := demux.ByKey(t.Context(), source(1), func(v int) int { return v }, func(_ int, ch <-chan int) {
		got = cap(ch)
		for range ch {
		}
	}, demux.WithBuffer(3))
	if err != nil {
		t.Fatalf("ByKey() = %v, want nil", err)
	}
	if got != 3 {
		t.Errorf("cap(ch) = %d, want 3", got)
	}
}

func TestOptionPanics(t *testing.T) {
	tests := map[string]func(){
		"negative max keys": func() { demux.WithMaxKeys(-1) },
		"negative buffer":   func() { demux.WithBuffer(-1) },
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
