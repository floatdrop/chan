package demux_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/floatdrop/chan/demux"
)

func ExampleByKey() {
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

	if err := demux.ByKey(context.Background(), in, customer, handle, demux.WithBuffer(8)); err != nil {
		panic(err)
	}

	// ByKey has waited for every handler to return.
	for _, name := range slices.Sorted(maps.Keys(items)) {
		fmt.Println(name, items[name])
	}
	// Output:
	// alice [cake jam]
	// bob [tea milk]
	// carol [tea]
}
