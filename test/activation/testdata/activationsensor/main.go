// Command activationsensor is the sensor the activation proof applies. It
// answers every evaluation with one trigger and a cursor one step past the one
// it was handed, so a committed tick shows up as a moved cursor and a fresh run
// key rather than as the same answer repeated.
//
// It is a fixture, not an example: the shortest program that satisfies the
// frozen sensor contract (docs/reference/sensor-contract.md) and leaves visible
// evidence in the database.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// cursorPrefix names the sequence. The counter after it is what moves.
const cursorPrefix = "event-"

// inbound is the part of the contract this sensor reads. The whole object
// arrives on stdin; the cursor is the only field the answer depends on.
type inbound struct {
	Cursor string `json:"cursor"`
}

type trigger struct {
	RunKey string `json:"run_key"`
}

// answer is the one JSON object the contract allows on stdout.
type answer struct {
	Cursor   string    `json:"cursor"`
	Triggers []trigger `json:"triggers"`
}

func main() {
	var in inbound
	// The contract carries the cursor twice, on stdin and in the
	// environment. Reading the second when the first will not decode keeps
	// the sequence moving instead of restarting it.
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		in.Cursor = os.Getenv("PACEQ_CURSOR")
	}
	next := step(in.Cursor)
	out, err := json.Marshal(answer{Cursor: next, Triggers: []trigger{{RunKey: next}}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode the answer:", err)
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(out); err != nil {
		fmt.Fprintln(os.Stderr, "write the answer:", err)
		os.Exit(1)
	}
}

// step returns the cursor after the given one. An empty or unreadable cursor
// starts the sequence, which is what the first evaluation of a fresh sensor
// gets.
func step(cursor string) string {
	n := 0
	if rest, ok := strings.CutPrefix(cursor, cursorPrefix); ok {
		if parsed, err := strconv.Atoi(rest); err == nil {
			n = parsed
		}
	}
	return cursorPrefix + strconv.Itoa(n+1)
}
