// Package batch groups values received from a channel into slices.
//
// A batch is sent when it reaches its size, when its timeout expires (see
// [WithTimeout]), when a split function says the next value starts a new batch
// (see [NewFunc]), or when the input channel is closed. Batches are never empty
// and never share memory: the receiver owns every slice it gets.
package batch

import (
	"context"
	"time"

	"github.com/floatdrop/chan/internal/ctxchan"
)

// Option configures [New] and [NewFunc].
type Option func(*config)

type config struct {
	timeout time.Duration
	buffer  int
}

// WithTimeout bounds how long a batch can stay unfinished: once d has passed
// since its first value arrived, the batch is sent even if it is not full.
// Without this option, a batch waits until it is full or the input is closed.
// To batch by time alone, pass math.MaxInt as the size.
//
// WithTimeout panics if d is not positive.
func WithTimeout(d time.Duration) Option {
	if d <= 0 {
		panic("batch: timeout must be positive")
	}
	return func(c *config) { c.timeout = d }
}

// WithBuffer sets the capacity of the output channel. The default is 0: the
// channel is unbuffered. WithBuffer panics if n is negative.
func WithBuffer(n int) Option {
	if n < 0 {
		panic("batch: buffer size must not be negative")
	}
	return func(c *config) { c.buffer = n }
}

// New reads values from in and sends them in batches of up to size values to
// the returned channel, in the order they were received.
//
// When in is closed, New sends the last, possibly shorter, batch and closes the
// channel. When ctx is done, New stops reading and closes the channel; the batch
// it was filling is discarded. To shut down without losing values, close in.
//
// New panics if size is less than 1.
func New[T any](ctx context.Context, in <-chan T, size int, opts ...Option) <-chan []T {
	return start(ctx, in, size, nil, opts)
}

// NewFunc is like [New], but it also ends a batch where split reports true:
// split is called with the last value of the current batch and the next value,
// and if it returns true the current batch is sent and next starts a new one.
//
// To group consecutive values that share a key, split on a change of key:
//
//	func(prev, next Event) bool { return prev.UserID != next.UserID }
func NewFunc[T any](ctx context.Context, in <-chan T, size int, split func(prev, next T) bool, opts ...Option) <-chan []T {
	return start(ctx, in, size, split, opts)
}

func start[T any](ctx context.Context, in <-chan T, size int, split func(prev, next T) bool, opts []Option) <-chan []T {
	if size < 1 {
		panic("batch: size must be at least 1")
	}
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	out := make(chan []T, cfg.buffer)
	go run(ctx, in, out, size, split, cfg.timeout)
	return out
}

// burst is how many waiting values run takes with non-blocking receives
// before it selects again, which is when it checks the timeout.
const burst = 64

func run[T any](ctx context.Context, in <-chan T, out chan<- []T, size int, split func(prev, next T) bool, timeout time.Duration) {
	defer close(out)

	var (
		batch []T
		// capHint is the capacity of the next batch: the length of the last
		// one sent. The first batch starts small, since size may be huge.
		capHint = min(size, 64)
		timer   *time.Timer
		expired <-chan time.Time // timer.C while a timeout is armed, nil otherwise
	)
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timer.Stop()
		defer timer.Stop()
	}

	// send sends the current batch, if any, and disarms the timeout.
	// It reports false if ctx was done before the batch could be sent.
	send := func() bool {
		if expired != nil {
			timer.Stop() // Since Go 1.23, a stopped timer never delivers a stale value.
			expired = nil
		}
		if len(batch) == 0 {
			return true
		}
		if ctxchan.Send(ctx, out, batch) != nil {
			return false
		}
		capHint, batch = len(batch), nil
		return true
	}

	// add appends v to the current batch. It sends the batch before v if split
	// says so, and after v if the batch is full. It reports false if ctx was
	// done before a batch could be sent.
	add := func(v T) bool {
		if split != nil && len(batch) > 0 && split(batch[len(batch)-1], v) && !send() {
			return false
		}
		if batch == nil {
			// Batches cut short by a timeout or split would waste a full-size
			// buffer; sizing each batch like the last one adapts to both.
			batch = make([]T, 0, capHint)
			if timer != nil {
				timer.Reset(timeout)
				expired = timer.C
			}
		}
		batch = append(batch, v)
		return len(batch) < size || send()
	}

	for {
		select {
		case v, ok := <-in:
			// Take the values that are already waiting with non-blocking
			// receives, which cost a fraction of this select. ctx is checked
			// after every value, the timeout at least every burst values.
			for n := 1; ; n++ {
				if !ok {
					send()
					return
				}
				if !add(v) || ctx.Err() != nil {
					return
				}
				if n == burst {
					break
				}
				select {
				case v, ok = <-in:
					continue
				default:
				}
				break
			}
		case <-expired:
			if !send() {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
