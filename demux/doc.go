// Package demux splits the values received from one channel by key.
//
// [ByKey] starts a consumer for each key it sees and gives it the values with
// that key, in order. Consumers of different keys run concurrently; values of
// one key are never processed concurrently.
package demux
