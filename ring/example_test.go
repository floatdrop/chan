package ring_test

import (
	"context"
	"fmt"

	"github.com/floatdrop/chan/ring"
)

func ExampleNew() {
	r := ring.New[int](3)
	for i := range 5 {
		r.Send(i) // Never blocks, even though nobody is receiving yet.
	}
	r.Close()

	for v := range r.C {
		fmt.Println(v)
	}
	fmt.Println("dropped:", r.Dropped())
	// Output:
	// 2
	// 3
	// 4
	// dropped: 2
}

func ExampleFrom() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Stops From if the loop below exits early.

	progress := make(chan int)
	go func() {
		defer close(progress)
		for pct := range 101 {
			progress <- pct
		}
	}()

	// The producer is never held up by a slow receiver; the receiver sees the
	// latest updates and skips the ones it was too slow for.
	latest := ring.From(ctx, progress, 1)
	last := 0
	for pct := range latest.C {
		last = pct // Render the progress bar.
	}
	fmt.Printf("finished at %d%%\n", last) // The newest value is never dropped.
	// Output: finished at 100%
}
