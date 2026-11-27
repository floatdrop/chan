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

// Receiver is the receiving side of a ring: its values and the count of those
// it dropped. [From] returns one, and every [Ring] embeds one. The zero value
// is not usable.
type Receiver[T any] struct {
	// C delivers the values in the order they were sent. It is closed when the
	// ring is closed; values still buffered at that point can be received
	// before the channel reports that it is closed.
	C <-chan T

	dropped atomic.Uint64
}

// Dropped reports how many values were discarded to make room for newer ones.
func (r *Receiver[T]) Dropped() uint64 {
	return r.dropped.Load()
}

// Ring is a buffered channel that keeps the newest values. [Ring.Send] never
// blocks: when the buffer is full, it discards the oldest value first.
//
// Values are received from C, which Ring has from its embedded [Receiver].
// Like a channel, a Ring may be used by several senders and receivers at once
// and is closed by a sender: Send and Close panic once the Ring is closed.
//
// A Ring must be created with [New]; the zero value is not usable.
type Ring[T any] struct {
	Receiver[T]

	ch chan T
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

// From creates a ring of the given size and starts a goroutine that sends
// every value received from in to it. When in is closed or ctx is done, the
// goroutine stops reading in and closes the ring.
//
// The ring belongs to that goroutine, so From returns only its receiving side.
// It panics if size is less than 1.
func From[T any](ctx context.Context, in <-chan T, size int) *Receiver[T] {
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
	return &r.Receiver
}

// Send adds v to the Ring without blocking. If the Ring is full, Send first
// discards the oldest value, which [Receiver.Dropped] counts.
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
