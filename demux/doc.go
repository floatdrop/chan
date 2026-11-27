// Package demux routes values received from one channel to many, by key.
//
// [Static] sends each value to one of a fixed set of channels. [Dynamic] starts
// a consumer for each key it sees and gives it the values with that key, in
// order. Both run in the caller's goroutine until the input channel is closed
// or the context is done, and close the channels they send to before returning.
package demux
