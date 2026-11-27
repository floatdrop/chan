# chan

[![CI](https://github.com/floatdrop/chan/actions/workflows/ci.yaml/badge.svg)](https://github.com/floatdrop/chan/actions/workflows/ci.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/floatdrop/chan.svg)](https://pkg.go.dev/github.com/floatdrop/chan)
[![Go Report Card](https://goreportcard.com/badge/github.com/floatdrop/chan)](https://goreportcard.com/report/github.com/floatdrop/chan)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generic channel utilities for Go 1.27+, with no dependencies.

```bash
go get github.com/floatdrop/chan@latest
```

## [batch](https://pkg.go.dev/github.com/floatdrop/chan/batch)

Groups values into slices by size, timeout or a split function.

```go
// Insert rows in batches of up to 1000, and never keep a row waiting for more than 50ms.
for rows := range batch.New(ctx, in, 1000, batch.WithTimeout(50*time.Millisecond)) {
	db.InsertMany(rows)
}
```

A batch is sent when it is full, when its timeout expires (counted from its first value), or when the input is closed. It is never empty, and the receiver owns the slice. `NewFunc` can also end a batch early, for example when the key changes:

```go
byUser := func(prev, next Event) bool { return prev.UserID != next.UserID }
for events := range batch.NewFunc(ctx, in, 100, byUser) {
	// events are consecutive and all belong to one user.
}
```

## [ring](https://pkg.go.dev/github.com/floatdrop/chan/ring)

A bounded channel that drops the oldest value instead of blocking the sender.

```go
r := ring.New[Metric](128)
r.Send(m) // Never blocks: when the ring is full, the oldest value is dropped.

for m := range r.C { // A slow receiver sees the newest values.
	report(m)
}
r.Dropped() // How many values were discarded.
```

`ring.From(ctx, in, size)` puts a ring in front of an existing channel. Its goroutine owns the ring, so `From` returns a `*ring.Receiver` that offers only `C` and `Dropped()`.

## [demux](https://pkg.go.dev/github.com/floatdrop/chan/demux)

Fans values out by key, to fixed channels or to a consumer started per key.

`Static` sends each value to one of a fixed set of channels and closes them all when the input ends:

```go
err := demux.Static(ctx, in, func(e Event) string { return e.Kind }, map[string]chan<- Event{
	"click": clicks,
	"view":  views,
})
```

`Dynamic` starts a consumer for each key it sees. Each consumer receives its key's values in order. `Dynamic` returns once every consumer has returned:

```go
err := demux.Dynamic(ctx, in, func(o Order) string { return o.CustomerID },
	func(customer string, orders <-chan Order) {
		for o := range orders {
			process(customer, o) // One customer's orders are processed one at a time.
		}
	},
	demux.WithMaxKeys(10_000), // Close the least recently used key beyond this limit.
	demux.WithBuffer(16),
)
```

## Benchmarks

```bash
go test -run='^$' -bench=. -benchmem ./...
```

One op is one value. The input channels are buffered, and the receivers do no work. The numbers are mostly channel hand-offs between goroutines.

```
goos: darwin
goarch: arm64
cpu: Apple M3 Max
pkg: github.com/floatdrop/chan/batch
BenchmarkNew/size=1-14                 2625022     443.9 ns/op     8 B/op   0 allocs/op
BenchmarkNew/size=16-14               14132894     84.89 ns/op     8 B/op   0 allocs/op
BenchmarkNew/size=256-14              29350527     41.02 ns/op     8 B/op   0 allocs/op
BenchmarkNewTimeout/size=1-14          2813750     422.6 ns/op     7 B/op   0 allocs/op
BenchmarkNewTimeout/size=16-14        12316239     100.6 ns/op     8 B/op   0 allocs/op
BenchmarkNewTimeout/size=256-14       27033140     43.46 ns/op     8 B/op   0 allocs/op
BenchmarkNewFunc-14                   15001366     79.33 ns/op     8 B/op   0 allocs/op
pkg: github.com/floatdrop/chan/demux
BenchmarkStatic/keys=1-14              8448268     122.1 ns/op     0 B/op   0 allocs/op
BenchmarkStatic/keys=16-14             7079841     171.4 ns/op     0 B/op   0 allocs/op
BenchmarkDynamic/keys=1-14             9604647     123.1 ns/op     0 B/op   0 allocs/op
BenchmarkDynamic/keys=16-14            6578126     183.7 ns/op     0 B/op   0 allocs/op
BenchmarkDynamic/keys=1024-14          4544674     264.1 ns/op     0 B/op   0 allocs/op
BenchmarkDynamicChurn-14               1466347     813.6 ns/op   872 B/op   5 allocs/op
pkg: github.com/floatdrop/chan/ring
BenchmarkSend/size=1-14               37045423     32.60 ns/op     0 B/op   0 allocs/op
BenchmarkSend/size=64-14              32368641     36.65 ns/op     0 B/op   0 allocs/op
BenchmarkSendFull-14                  52008841     22.81 ns/op     0 B/op   0 allocs/op
BenchmarkSendParallel-14              24826450     49.27 ns/op     0 B/op   0 allocs/op
BenchmarkFrom-14                      10233439     116.7 ns/op     0 B/op   0 allocs/op
BenchmarkChan-14                      39370562     30.04 ns/op     0 B/op   0 allocs/op
```

Compared with the archived packages on the same machine (median of 6 runs):

| Benchmark | Before | After | Speedup |
|---|---:|---:|---:|
| `batch/New/size=1` | 306 ns | 415 ns | 0.74× |
| `batch/New/size=16` | 91 ns | 85 ns | 1.08× |
| `batch/New/size=256` | 58 ns | 41 ns | 1.42× |
| `batch/NewTimeout/size=1` | 342 ns | 434 ns | 0.79× |
| `batch/NewTimeout/size=16` | 157 ns | 102 ns | 1.54× |
| `batch/NewTimeout/size=256` | 95 ns | 43 ns | 2.20× |
| `batch/NewFunc` | 112 ns | 80 ns | 1.40× (128 → 8 B/op) |
| `demux/Static/keys=1` | 90 ns | 120 ns | 0.75× |
| `demux/Static/keys=16` | 153 ns | 171 ns | 0.89× |
| `demux/Dynamic/keys=1` | 92 ns | 123 ns | 0.75× |
| `demux/Dynamic/keys=16` | 167 ns | 180 ns | 0.93× |
| `demux/Dynamic/keys=1024` | 267 ns | 284 ns | 0.94× |
| `demux/DynamicChurn` | 723 ns | 889 ns | 0.81× (776 → 872 B/op) |
| `ring/From` | 103 ns | 116 ns | 0.89× |

- The old packages could not be canceled. Waiting on a cancelable context costs `demux` and `ring.From` 10–25%. With `context.Background()`, which can never be canceled, they use plain channel operations and run at 0.88–1.04× of the old speed (churn aside).
- `batch` with `size=1` is slower because its output channel is now unbuffered by default. `WithBuffer(1)` brings it to about 340 ns.
- `DynamicChurn` reopens a key for nearly every value. The extra time there is what keeps a reopened key's consumers from running at the same time.
- `ring.New` with `Send` costs about as much as a plain buffered channel send (`BenchmarkChan`), because it involves no goroutine.

## License

MIT, see [LICENSE](LICENSE).
