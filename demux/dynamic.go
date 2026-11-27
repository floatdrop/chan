package demux

import (
	"context"
	"sync"

	"github.com/floatdrop/chan/internal/ctxchan"
)

// Option configures [Dynamic].
type Option func(*config)

type config struct {
	maxKeys int
	buffer  int
}

// WithMaxKeys limits [Dynamic] to n open keys. When a value with a new key
// arrives and n keys are open, the least recently used key is closed: its
// channel is closed, so its consumer can finish. If that key comes back, a new
// consumer is started for it.
//
// The default, 0, means no limit. WithMaxKeys panics if n is negative.
func WithMaxKeys(n int) Option {
	if n < 0 {
		panic("demux: max keys must not be negative")
	}
	return func(c *config) { c.maxKeys = n }
}

// WithBuffer sets the capacity of each key's channel. The default is 0: the
// channels are unbuffered. WithBuffer panics if n is negative.
func WithBuffer(n int) Option {
	if n < 0 {
		panic("demux: buffer size must not be negative")
	}
	return func(c *config) { c.buffer = n }
}

// Dynamic routes values received from in to consumers it starts for each key.
//
// The first value with a new key starts a goroutine running consume(key, ch).
// Channel ch delivers that value and every later value with the same key, in
// the order they were received. A key's consumers never run at the same time:
// when a key closed by [WithMaxKeys] comes back, its new consumer starts only
// after the previous one has returned.
//
// Dynamic runs until in is closed or ctx is done. It then closes every channel,
// waits for every consumer to return, and returns ctx.Err() if ctx ended it, or
// nil otherwise. Values already buffered in a channel are still delivered.
//
// Values are sent one at a time, so a consumer that falls behind holds up the
// others. [WithBuffer] gives each consumer room to absorb short delays.
func Dynamic[T any, K comparable](ctx context.Context, in <-chan T, key func(T) K, consume func(K, <-chan T), opts ...Option) error {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	d := &dispatcher[T, K]{
		config:  cfg,
		consume: consume,
		open:    make(map[K]*route[T, K]),
		running: make(map[K]*route[T, K]),
	}
	d.lru.init()
	defer d.shutdown()

	for {
		v, ok, err := ctxchan.Recv(ctx, in)
		if err != nil || !ok {
			return err
		}
		if err := ctxchan.Send(ctx, d.routeFor(key(v)).ch, v); err != nil {
			return err
		}
	}
}

// A route carries the values of one key to one consumer.
type route[T any, K comparable] struct {
	key        K
	ch         chan T
	done       chan struct{} // closed when the consumer has returned
	prev, next *route[T, K]  // neighbors in dispatcher.lru
}

type dispatcher[T any, K comparable] struct {
	config
	consume func(K, <-chan T)
	open    map[K]*route[T, K] // routes whose channel is open
	lru     list[T, K]         // open routes, most recently used first
	wg      sync.WaitGroup

	mu      sync.Mutex
	running map[K]*route[T, K] // per key, the newest route whose consumer has not returned
}

// routeFor returns the open route for k, opening one if there is none.
func (d *dispatcher[T, K]) routeFor(k K) *route[T, K] {
	if r, ok := d.open[k]; ok {
		d.lru.moveToFront(r)
		return r
	}
	if d.maxKeys > 0 && len(d.open) >= d.maxKeys {
		d.evict(d.lru.back())
	}
	r := &route[T, K]{key: k, ch: make(chan T, d.buffer), done: make(chan struct{})}
	d.open[k] = r
	d.lru.pushFront(r)
	d.start(r)
	return r
}

// start runs the consumer of r once the previous consumer of its key, if any,
// has returned.
func (d *dispatcher[T, K]) start(r *route[T, K]) {
	d.mu.Lock()
	prev := d.running[r.key]
	d.running[r.key] = r
	d.mu.Unlock()

	d.wg.Go(func() {
		if prev != nil {
			<-prev.done
		}
		d.consume(r.key, r.ch)

		d.mu.Lock()
		if d.running[r.key] == r {
			delete(d.running, r.key)
		}
		d.mu.Unlock()
		close(r.done)
	})
}

func (d *dispatcher[T, K]) evict(r *route[T, K]) {
	d.lru.remove(r)
	delete(d.open, r.key)
	close(r.ch)
}

func (d *dispatcher[T, K]) shutdown() {
	for _, r := range d.open {
		close(r.ch)
	}
	d.wg.Wait()
}

// list is a doubly linked list of routes, threaded through their prev and
// next fields. It must be initialized with init before use.
type list[T any, K comparable] struct {
	root route[T, K] // sentinel: root.next is the front, root.prev the back
}

func (l *list[T, K]) init() {
	l.root.prev, l.root.next = &l.root, &l.root
}

func (l *list[T, K]) pushFront(r *route[T, K]) {
	r.prev, r.next = &l.root, l.root.next
	r.prev.next, r.next.prev = r, r
}

func (l *list[T, K]) remove(r *route[T, K]) {
	r.prev.next, r.next.prev = r.next, r.prev
	r.prev, r.next = nil, nil
}

func (l *list[T, K]) moveToFront(r *route[T, K]) {
	if l.root.next != r {
		l.remove(r)
		l.pushFront(r)
	}
}

func (l *list[T, K]) back() *route[T, K] {
	return l.root.prev
}
