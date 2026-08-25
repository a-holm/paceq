package reason

import "testing"

// The output warnings (#13) are the first codes that are deliberately NOT
// terminal: they name something worth reading beside a step that still
// succeeded. The catalogue must hold them at the step level with Terminal
// unset, because a parse warning in $PACEQ_OUTPUT never ends a step by
// itself (issue design choice 5).
func TestOutputWarningCodesAreWarnings(t *testing.T) {
	cases := []struct {
		code  Code
		short string
	}{
		{STEPOutputInvalid, "output had lines that could not be read"},
		{STEPOutputTruncated, "output was cut off at a bound"},
	}
	for _, tc := range cases {
		entry, ok := Lookup(tc.code)
		if !ok {
			t.Errorf("%s is not in the catalogue", tc.code)
			continue
		}
		if entry.Code != tc.code {
			t.Errorf("catalogue entry code = %s, want %s", entry.Code, tc.code)
		}
		if entry.Level != LevelStep {
			t.Errorf("%s level = %v, want step level", tc.code, entry.Level)
		}
		if entry.Terminal {
			t.Errorf("%s is marked terminal: an output warning must never end a step", tc.code)
		}
		if entry.Short != tc.short {
			t.Errorf("%s short = %q, want %q", tc.code, entry.Short, tc.short)
		}
		if len(entry.Remedy) == 0 {
			t.Errorf("%s carries no remedy", tc.code)
		}
	}
}
