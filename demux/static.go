package demux

import (
	"context"

	"github.com/floatdrop/chan/internal/ctxchan"
)

// Static sends each value v received from in to outs[key(v)], and discards
// values whose key is not in outs.
//
// Static runs until in is closed or ctx is done, then closes every channel in
// outs and returns. It returns ctx.Err() if ctx ended it, and nil otherwise.
// Several keys may share a channel; it is closed once.
//
// Values are sent one at a time, so a receiver that falls behind holds up
// the others. Give the channels buffers to absorb short delays.
func Static[T any, K comparable](ctx context.Context, in <-chan T, key func(T) K, outs map[K]chan<- T) error {
	defer closeAll(outs)

	for {
		v, ok, err := ctxchan.Recv(ctx, in)
		if err != nil || !ok {
			return err
		}
		if out, ok := outs[key(v)]; ok {
			if err := ctxchan.Send(ctx, out, v); err != nil {
				return err
			}
		}
	}
}

func closeAll[T any, K comparable](outs map[K]chan<- T) {
	closed := make(map[chan<- T]bool, len(outs))
	for _, ch := range outs {
		if !closed[ch] {
			closed[ch] = true
			close(ch)
		}
	}
}
