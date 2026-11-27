// Package ring provides a bounded channel that never blocks its sender: when
// the buffer is full, a send discards the oldest value to make room.
//
// It suits streams where the newest values matter most, such as metrics,
// progress reports or UI state, and a slow receiver must not stall the sender.
package ring

import (
	"context"
	"sync/atomic"

	"github.com/floatdrop/chan/internal/ctxchan"
)

// Ring is a buffered channel that keeps the newest values. [Ring.Send] never
// blocks: when the buffer is full, it discards the oldest value first.
//
// Values are received from C in the order they were sent. Like a channel, a
// Ring may be used by several senders and receivers at once and is closed by a
// sender: Send and Close panic once the Ring is closed.
//
// A Ring must be created with [New] or [From]; the zero value is not usable.
type Ring[T any] struct {
	// C delivers the values. It is closed by Close; values still buffered at
	// that point can be received before the channel reports that it is closed.
	C <-chan T

	ch      chan T
	dropped atomic.Uint64
}

// New returns an empty Ring that holds up to size values.
// It panics if size is less than 1.
func New[T any](size int) *Ring[T] {
	if size < 1 {
		panic("ring: size must be at least 1")
	}
	ch := make(chan T, size)
	return &Ring[T]{C: ch, ch: ch}
}

// From returns a Ring of the given size and starts a goroutine that sends
// every value received from in to it. When in is closed or ctx is done, the
// goroutine stops reading in and closes the Ring. The Ring belongs to that
// goroutine: the caller must not call Send or Close.
//
// It panics if size is less than 1.
func From[T any](ctx context.Context, in <-chan T, size int) *Ring[T] {
	r := New[T](size)
	go func() {
		defer r.Close()
		for {
			v, ok, err := ctxchan.Recv(ctx, in)
			if err != nil || !ok {
				return
			}
			r.Send(v)
		}
	}()
	return r
}

// Send adds v to the Ring without blocking. If the Ring is full, Send first
// discards the oldest value, which [Ring.Dropped] counts.
func (r *Ring[T]) Send(v T) {
	for {
		select {
		case r.ch <- v:
			return
		default:
		}
		// The buffer is full. Discard the oldest value, unless receivers have
		// emptied the buffer in the meantime, then try again.
		select {
		case <-r.ch:
			r.dropped.Add(1)
		default:
		}
	}
}

// Close closes C. Values already in the Ring can still be received.
func (r *Ring[T]) Close() {
	close(r.ch)
}

// Dropped reports how many values Send has discarded to make room.
func (r *Ring[T]) Dropped() uint64 {
	return r.dropped.Load()
}
