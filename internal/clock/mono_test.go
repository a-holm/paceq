package clock_test

import (
	"encoding"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// TestMonoCannotLeaveTheProcess pins the properties that stop a monotonic mark
// from reaching the database or the wire. See the Mono doc comment for what the
// type deliberately does not prevent.
func TestMonoCannotLeaveTheProcess(t *testing.T) {
	mono := reflect.TypeOf(clock.Mono{})

	for i := range mono.NumField() {
		if f := mono.Field(i); f.IsExported() {
			t.Errorf("Mono has exported field %q: a Mono must carry no data out of the process", f.Name)
		}
	}

	for _, name := range []string{"MarshalJSON", "MarshalText", "MarshalBinary", "Value", "Unix", "UnixNano"} {
		if _, ok := mono.MethodByName(name); ok {
			t.Errorf("Mono has method %s: it makes a monotonic mark storable", name)
		}
	}

	if reflect.TypeOf(clock.Mono{}).ConvertibleTo(reflect.TypeOf(time.Time{})) {
		t.Error("Mono converts to time.Time: the compiler must refuse that mistake")
	}

	if _, ok := any(clock.Mono{}).(json.Marshaler); ok {
		t.Error("Mono implements json.Marshaler")
	}
	if _, ok := any(clock.Mono{}).(encoding.TextMarshaler); ok {
		t.Error("Mono implements encoding.TextMarshaler")
	}
}

// TestMonoSerialisesToNothing states the honest limit: encoding a Mono is not an
// error, it simply loses everything, so a mark that made a round trip measures
// from the clock's origin instead of from where it was taken.
func TestMonoSerialisesToNothing(t *testing.T) {
	f := clock.NewFake(t0)
	f.Advance(time.Hour)
	mark := f.Mark()

	//lint:ignore SA9005 encoding a type with no exported fields is what this test asserts
	encoded, err := json.Marshal(mark)
	if err != nil {
		t.Fatalf("json.Marshal(Mono): %v", err)
	}
	if string(encoded) != "{}" {
		t.Errorf("json.Marshal(Mono) = %s, want {}", encoded)
	}

	var restored clock.Mono
	//lint:ignore SA9005 decoding into a type with no exported fields is what this test asserts
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("json.Unmarshal into Mono: %v", err)
	}
	if got, want := f.Since(restored), time.Hour; got != want {
		t.Errorf("Since(round-tripped mark) = %v, want %v: the mark must arrive empty", got, want)
	}
}
