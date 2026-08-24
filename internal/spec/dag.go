package spec

import (
	"strings"
)

// The step graph. Needs on a step is a dependency edge from the step that
// carries it to the step it names: if step B lists A in Needs, B can only
// start once A has succeeded. A graph built this way has a valid scheduling
// order exactly when it has no cycle, and TopoOrder is what finds either. It
// is kept a pure function of the steps so the parser, the CLI and the store
// all share one copy of the rule and cannot disagree about what a graph
// means.

// TopoOrder returns the deterministic order the steps may run in, and, when
// the graph has a cycle, the name of a printed cycle path that proves it.
//
// The order is deterministic by construction: at every step the ready steps
// are the ones whose needs have all been emitted, and among several ready the
// one whose name sorts first is chosen. Nothing about it may depend on the
// order the steps arrived in, because the same job must hash, validate and
// run the same everywhere. The result has length equal to the steps only when
// the graph is acyclic; a cycle stops the sort with the steps it has emitted,
// and 'cycle' is set to a path of the form "a -> b -> a".
func TopoOrder(steps []Step) (order []string, cycle string) {
	// byName maps a step name to its index in steps.
	byName := make(map[string]int, len(steps))
	order = make([]string, 0, len(steps))
	for i, s := range steps {
		byName[s.Name] = i
	}

	// children[i] is the set of step indices that wait on step i.
	children := make([][]int, len(steps))
	// waiting[i] is how many needs step i still has outstanding.
	waiting := make([]int, len(steps))

	for _, s := range steps {
		idx, _ := byName[s.Name]
		for _, need := range s.Needs {
			if need == s.Name {
				// A step that needs itself is a one-step cycle. Naming the
				// own step twice makes the path read clearly.
				return order, need + " -> " + need
			}
			parent, ok := byName[need]
			if !ok {
				continue
			}
			children[parent] = append(children[parent], idx)
			waiting[idx]++
		}
	}

	// ready holds the indices of steps with nothing outstanding, in name
	// order, so the tie-break is fixed before the graph is touched.
	ready := make([]int, 0, len(steps))
	for i := range steps {
		if waiting[i] == 0 {
			ready = append(ready, i)
		}
	}

	// A binary heap would be the steady state shape, but the graph is bounded
	// by MaxSteps (200, spec.go), so a linear scan for the smallest name that
	// is ready is bounded work with no heap bookkeeping to get wrong.
	for len(ready) > 0 {
		next := 0
		for i := 1; i < len(ready); i++ {
			if steps[ready[i]].Name < steps[ready[next]].Name {
				next = i
			}
		}
		idx := ready[next]
		// Remove idx from ready by swapping the last one in.
		ready[next] = ready[len(ready)-1]
		ready = ready[:len(ready)-1]

		order = append(order, steps[idx].Name)
		for _, child := range children[idx] {
			waiting[child]--
			if waiting[child] == 0 {
				ready = append(ready, child)
			}
		}
	}

	if len(order) < len(steps) {
		return order, findCycle(steps, byName, waiting)
	}
	return order, ""
}

// findCycle walks the steps that Kahn had to leave behind, following their
// outstanding needs, until it comes back to a step it has seen. The names it
// crossed are the cycle, printed with an arrow between them so a reader can
// verify it by reading the file.
func findCycle(steps []Step, byName map[string]int, waiting []int) string {
	seen := make(map[string]bool, len(steps))
	for _, s := range steps {
		if waiting[byName[s.Name]] == 0 || seen[s.Name] {
			continue
		}
		path := make([]string, 0, len(steps))
		onPath := make(map[string]bool, len(steps))
		at := s.Name
		for {
			if onPath[at] {
				// Rotate so the repeated name frames the cycle.
				start := 0
				for i, name := range path {
					if name == at {
						start = i
						break
					}
				}
				path = path[start:]
				path = append(path, at)
				return strings.Join(path, " -> ")
			}
			if seen[at] {
				break
			}
			seen[at] = true
			onPath[at] = true
			path = append(path, at)
			idx := byName[at]
			next := ""
			for _, need := range steps[idx].Needs {
				if waiting[byName[need]] > 0 {
					next = need
					break
				}
			}
			if next == "" {
				break
			}
			at = next
		}
	}
	return ""
}
