package ctxchan_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/floatdrop/chan/internal/ctxchan"
)

func TestRecv(t *testing.T) {
	in := make(chan int, 1)
	in <- 1
	if v, ok, err := ctxchan.Recv(t.Context(), in); v != 1 || !ok || err != nil {
		t.Errorf("Recv() = %v, %v, %v, want 1, true, nil", v, ok, err)
	}
	close(in)
	if _, ok, err := ctxchan.Recv(t.Context(), in); ok || err != nil {
		t.Errorf("Recv() on closed channel = _, %v, %v, want false, nil", ok, err)
	}
}

func TestBackground(t *testing.T) {
	ch := make(chan int, 1)
	if err := ctxchan.Send(context.Background(), ch, 1); err != nil {
		t.Fatalf("Send() = %v, want nil", err)
	}
	if v, ok, err := ctxchan.Recv(context.Background(), ch); v != 1 || !ok || err != nil {
		t.Errorf("Recv() = %v, %v, %v, want 1, true, nil", v, ok, err)
	}
}

func TestRecvCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	in := make(chan int, 1)
	in <- 1
	if _, _, err := ctxchan.Recv(ctx, in); !errors.Is(err, context.Canceled) {
		t.Errorf("Recv() error = %v, want %v", err, context.Canceled)
	}
	if len(in) != 1 {
		t.Error("Recv() took a value after ctx was done")
	}
}

func TestRecvBlockedThenCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		errc := make(chan error)
		go func() {
			_, _, err := ctxchan.Recv(ctx, make(chan int))
			errc <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("Recv() error = %v, want %v", err, context.Canceled)
		}
	})
}

func TestSend(t *testing.T) {
	ch := make(chan int, 1)
	if err := ctxchan.Send(t.Context(), ch, 1); err != nil {
		t.Fatalf("Send() = %v, want nil", err)
	}
	if v := <-ch; v != 1 {
		t.Errorf("received %d, want 1", v)
	}
}

func TestSendCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ch := make(chan int, 1)
	if err := ctxchan.Send(ctx, ch, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("Send() = %v, want %v", err, context.Canceled)
	}
	if len(ch) != 0 {
		t.Error("Send() sent a value after ctx was done")
	}
}

func TestSendBlockedThenCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		errc := make(chan error)
		go func() { errc <- ctxchan.Send(ctx, make(chan int), 1) }()
		synctest.Wait()
		cancel()
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("Send() = %v, want %v", err, context.Canceled)
		}
	})
}
