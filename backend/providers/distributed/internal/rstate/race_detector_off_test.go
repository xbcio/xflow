//go:build !race

package rstate

// raceDetectorEnabled lets the replay-heavy race tests shrink their round count
// when the race detector multiplies the cost of every round.
const raceDetectorEnabled = false
