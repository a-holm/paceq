package reason

import (
	"strings"
	"testing"
)

// MissingDataKeys is what turns DataKeys from a line in the reference into
// something a writer can be held to (#191). These cases pin the answers the
// store and the engine guards lean on, including the two that are easy to get
// backwards: an empty payload owes every key, and a code outside the
// catalogue owes none.
func TestMissingDataKeysHoldsAPayloadAgainstItsPromise(t *testing.T) {
	cases := []struct {
		name string
		code Code
		data string
		want []string
	}{
		{
			name: "every promised key present",
			code: RUNPoisoned,
			data: `{"crash_count":6,"max_crash_count":5}`,
		},
		{
			name: "extra keys are allowed",
			code: RUNPoisoned,
			data: `{"crash_count":6,"max_crash_count":5,"attempt":2}`,
		},
		{
			name: "one key short",
			code: RUNPoisoned,
			data: `{"crash_count":6}`,
			want: []string{"max_crash_count"},
		},
		{
			name: "the empty object owes everything",
			code: RUNPoisoned,
			data: "{}",
			want: []string{"crash_count", "max_crash_count"},
		},
		{
			name: "no payload at all owes everything",
			code: RUNPoisoned,
			data: "",
			want: []string{"crash_count", "max_crash_count"},
		},
		{
			name: "a JSON null owes everything",
			code: RUNPoisoned,
			data: "null",
			want: []string{"crash_count", "max_crash_count"},
		},
		{
			name: "a payload that is not an object owes everything",
			code: RUNPoisoned,
			data: `["crash_count"]`,
			want: []string{"crash_count", "max_crash_count"},
		},
		{
			name: "a key with a null value still counts as carried",
			code: RUNFailedStep,
			data: `{"attempt":null,"step":null}`,
		},
		{
			name: "a code that promises nothing is never short",
			code: RUNSucceeded,
			data: "",
		},
		{
			name: "a code outside the catalogue promises nothing",
			code: Code("RUN_NO_SUCH_CODE"),
			data: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MissingDataKeys(tc.code, tc.data)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("MissingDataKeys(%s, %q) = %v, want %v", tc.code, tc.data, got, tc.want)
			}
		})
	}
}

// The order of the answer is the catalogue's own order, because the failure
// message a guard prints is read beside the reference page.
func TestMissingDataKeysAnswersInCatalogueOrder(t *testing.T) {
	entry, ok := Lookup(RUNRejectedDiskLow)
	if !ok {
		t.Fatal("RUN_REJECTED_DISK_LOW is not in the catalogue")
	}
	got := MissingDataKeys(RUNRejectedDiskLow, "{}")
	if strings.Join(got, ",") != strings.Join(entry.DataKeys, ",") {
		t.Errorf("missing keys came back as %v, want the declared order %v", got, entry.DataKeys)
	}
}
