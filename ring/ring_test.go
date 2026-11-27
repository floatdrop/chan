package ring_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/floatdrop/chan/ring"
)

func TestSendKeepsNewest(t *testing.T) {
	r := ring.New[int](3)
	for i := range 5 {
		r.Send(i)
	}
	r.Close()

	if got, want := collect(r.C), []int{2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := r.Dropped(); got != 2 {
		t.Errorf("Dropped() = %d, want 2", got)
	}
}

func TestSendBelowCapacity(t *testing.T) {
	r := ring.New[int](3)
	r.Send(1)
	r.Send(2)
	r.Close()

	if got, want := collect(r.C), []int{1, 2}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := r.Dropped(); got != 0 {
		t.Errorf("Dropped() = %d, want 0", got)
	}
}

func TestConcurrentSenders(t *testing.T) {
	const senders, perSender = 8, 10_000
	type msg struct{ sender, seq int }

	r := ring.New[msg](16)
	received := make(chan int)
	go func() {
		n := 0
		last := make(map[int]int)
		for m := range r.C {
			if prev, ok := last[m.sender]; ok && m.seq <= prev {
				t.Errorf("sender %d: got %d after %d", m.sender, m.seq, prev)
			}
			last[m.sender] = m.seq
			n++
		}
		received <- n
	}()

	var wg sync.WaitGroup
	for s := range senders {
		wg.Go(func() {
			for i := range perSender {
				r.Send(msg{s, i})
			}
		})
	}
	wg.Wait()
	r.Close()

	n := <-received
	if total := uint64(n) + r.Dropped(); total != senders*perSender {
		t.Errorf("received %d + dropped %d = %d, want %d", n, r.Dropped(), total, senders*perSender)
	}
}

func TestFrom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := make(chan int)
		r := ring.From(t.Context(), in, 2)
		for i := range 5 {
			in <- i
		}
		synctest.Wait() // From has stored every value.
		close(in)

		if got, want := collect(r.C), []int{3, 4}; !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got := r.Dropped(); got != 3 {
			t.Errorf("Dropped() = %d, want 3", got)
		}
	})
}

func TestFromCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		in := make(chan int, 1)
		r := ring.From(ctx, in, 2)

		cancel()
		synctest.Wait()
		in <- 1
		if _, ok := <-r.C; ok {
			t.Error("ring is open after cancel")
		}
		if len(in) != 1 {
			t.Error("From kept reading after cancel")
		}
	})
}

func TestNewPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New(0) did not panic")
		}
	}()
	ring.New[int](0)
}

func collect[T any](ch <-chan T) []T {
	var all []T
	for v := range ch {
		all = append(all, v)
	}
	return all
}
