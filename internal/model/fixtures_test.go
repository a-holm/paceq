package model_test

import "github.com/a-holm/paceq/internal/model"

// The timestamps the tables run against. Nothing in the model reads a clock, so
// all these values do is order available_at against now. The unit is unix
// milliseconds UTC, the unit of every time column in the database.
const (
	now    = int64(1_700_000_000_000)
	past   = now - 1000
	future = now + 1000
)

// Stand-ins for catalogues other issues own: reason codes are M1-05 and defer
// reasons belong to the gate that defers. The model only insists that the
// string is not empty.
const (
	reasonCode   = "run_finished"
	deferBecause = "concurrency_limit"
)

// kinds is an effect list with no arguments, which is most of them.
func kinds(list ...model.EffectKind) []model.Effect {
	out := make([]model.Effect, 0, len(list))
	for _, k := range list {
		out = append(out, model.Effect{Kind: k})
	}
	return out
}

// with appends argument carrying effects to a list of plain ones.
func with(base []model.Effect, extra ...model.Effect) []model.Effect {
	return append(base, extra...)
}

func emit(name string) model.Effect {
	return model.Effect{Kind: model.EffectEmit, Arg: name}
}

func deferTo(reason string) model.Effect {
	return model.Effect{Kind: model.EffectSetDeferReason, Arg: reason}
}

// errText is an error as a caller sees it, or "" for no error at all. Comparing
// the whole message is how the tests compare two outcomes without caring which
// refusal type carried them.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
