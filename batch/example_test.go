package batch_test

import (
	"context"
	"fmt"
	"time"

	"github.com/floatdrop/chan/batch"
)

func ExampleNew() {
	in := make(chan string, 5)
	for _, v := range []string{"A", "B", "C", "D", "E"} {
		in <- v
	}
	close(in)

	for b := range batch.New(context.Background(), in, 3) {
		fmt.Println(b)
	}
	// Output:
	// [A B C]
	// [D E]
}

func ExampleNewFunc() {
	type event struct{ user, action string }

	in := make(chan event, 5)
	for _, e := range []event{
		{"alice", "login"}, {"alice", "view"}, {"bob", "login"}, {"alice", "logout"}, {"bob", "logout"},
	} {
		in <- e
	}
	close(in)

	sameUser := func(prev, next event) bool { return prev.user != next.user }
	for b := range batch.NewFunc(context.Background(), in, 100, sameUser) {
		fmt.Println(b)
	}
	// Output:
	// [{alice login} {alice view}]
	// [{bob login}]
	// [{alice logout}]
	// [{bob logout}]
}

func ExampleWithTimeout() {
	ctx := context.Background()
	rows := make(chan string)
	go func() {
		defer close(rows)
		rows <- "first"
		rows <- "second"
		time.Sleep(300 * time.Millisecond) // A lull: the batch is sent before it fills up.
		rows <- "third"
	}()

	// Insert rows in batches of 1000, but never keep a row waiting for more than 50ms.
	for b := range batch.New(ctx, rows, 1000, batch.WithTimeout(50*time.Millisecond)) {
		fmt.Println(b)
	}
	// Output:
	// [first second]
	// [third]
}
