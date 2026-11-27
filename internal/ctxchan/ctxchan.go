// Package ctxchan provides channel operations that give up when a context is
// done.
//
// Each operation first checks ctx.Err(), an atomic load, so a call made after
// ctx is done leaves the channel alone even if it is ready. It then tries a
// non-blocking send or receive, which costs a fraction of a select, and falls
// back to selecting on the channel and ctx.Done() only when it would block.
// A context that can never be done, such as context.Background(), gets a plain
// send or receive.
package ctxchan

import "context"

// Recv receives a value from in, like v, ok := <-in, unless ctx is done:
// then it returns ctx.Err() and v and ok are meaningless.
func Recv[T any](ctx context.Context, in <-chan T) (v T, ok bool, err error) {
	done := ctx.Done()
	if done == nil {
		v, ok = <-in
		return v, ok, nil
	}
	if err = ctx.Err(); err != nil {
		return v, false, err
	}
	select {
	case v, ok = <-in:
		return v, ok, nil
	default:
	}
	select {
	case v, ok = <-in:
		return v, ok, nil
	case <-done:
		return v, false, ctx.Err()
	}
}

// Send sends v on ch, unless ctx is done: then it returns ctx.Err().
func Send[T any](ctx context.Context, ch chan<- T, v T) error {
	done := ctx.Done()
	if done == nil {
		ch <- v
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case ch <- v:
		return nil
	default:
	}
	select {
	case ch <- v:
		return nil
	case <-done:
		return ctx.Err()
	}
}
