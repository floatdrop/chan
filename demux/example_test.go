package demux_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/floatdrop/chan/demux"
)

func ExampleStatic() {
	in := make(chan int, 6)
	for i := range 6 {
		in <- i
	}
	close(in)

	even, odd := make(chan int, 6), make(chan int, 6)
	isEven := func(v int) bool { return v%2 == 0 }
	err := demux.Static(context.Background(), in, isEven, map[bool]chan<- int{true: even, false: odd})
	if err != nil {
		panic(err)
	}

	// Static has closed both channels, so ranging over them ends.
	var evens, odds []int
	for v := range even {
		evens = append(evens, v)
	}
	for v := range odd {
		odds = append(odds, v)
	}
	fmt.Println("even:", evens)
	fmt.Println("odd:", odds)
	// Output:
	// even: [0 2 4]
	// odd: [1 3 5]
}

func ExampleDynamic() {
	type order struct{ customer, item string }

	in := make(chan order, 5)
	for _, o := range []order{
		{"bob", "tea"}, {"alice", "cake"}, {"bob", "milk"}, {"carol", "tea"}, {"alice", "jam"},
	} {
		in <- o
	}
	close(in)

	var (
		mu    sync.Mutex
		items = make(map[string][]string)
	)
	customer := func(o order) string { return o.customer }
	// Each customer's orders are handled in their own goroutine, in order.
	handle := func(name string, orders <-chan order) {
		var got []string
		for o := range orders {
			got = append(got, o.item)
		}
		mu.Lock()
		items[name] = got
		mu.Unlock()
	}

	if err := demux.Dynamic(context.Background(), in, customer, handle, demux.WithBuffer(8)); err != nil {
		panic(err)
	}

	// Dynamic has waited for every handler to return.
	for _, name := range slices.Sorted(maps.Keys(items)) {
		fmt.Println(name, items[name])
	}
	// Output:
	// alice [cake jam]
	// bob [tea milk]
	// carol [tea]
}
