package spec

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
)

// The limits, checked on the syntax tree before anything is expanded (08 T13).
//
// This is the whole answer to the billion laughs file. An alias in a syntax
// tree is a name, not a copy: the tree of a file that expands to a terabyte is
// the size of the file it was written in. So the tree is measured first, and a
// file that would explode is refused having allocated nothing but itself.
//
// The survey also collects the anchors, refuses the two YAML features paceq
// does not honour, and stops at the first limit it hits. Reporting the depth
// and the node count of a file that is already too deep to read is noise.

// surveyItem is one node waiting to be measured, with the nesting depth it sits
// at. The walk keeps its own stack rather than recursing, so the depth of the
// walk is a number in this package rather than a property of the machine.
type surveyItem struct {
	node  ast.Node
	depth int
}

func (d *decoder) survey(root ast.Node) {
	stack := []surveyItem{{node: root, depth: 1}}
	nodes, aliases := 0, 0

	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if item.node == nil {
			continue
		}

		nodes++
		if nodes > MaxNodes {
			d.error(CodeTooLarge, position(item.node),
				fmt.Sprintf("the job file has more than %d nodes", MaxNodes),
				"Split the job into several files. A definition this size is generated, and\n"+
					"paceq would rather read the generator's output one job at a time.")
			return
		}
		if item.depth > MaxDepth {
			d.error(CodeTooDeep, position(item.node),
				fmt.Sprintf("the job file nests %d levels deep, and paceq reads at most %d", item.depth, MaxDepth),
				"A job definition needs six levels. Anything past that is a data structure\n"+
					"that belongs in a file the job reads, not in the job.")
			return
		}

		switch n := item.node.(type) {
		case *ast.AliasNode:
			aliases++
			if aliases > MaxAliases {
				d.error(CodeTooManyAliases, position(n),
					fmt.Sprintf("the job file uses more than %d aliases", MaxAliases),
					"Anchors are for sharing one env block between a few steps. A file that needs\n"+
						"a hundred of them is a program, and paceq does not run programs to find out\n"+
						"what a job is.")
				return
			}
		case *ast.AnchorNode:
			if !d.declareAnchor(n) {
				return
			}
		case *ast.TagNode:
			d.error(CodeTag, position(n),
				fmt.Sprintf("the tag %q is not one paceq honours", n.Start.Value),
				"Remove the tag. Every field in a job has one type already, and paceq reads the\n"+
					"value as that type. A tag can only disagree with it.")
			return
		}

		depth := item.depth
		if isContainer(item.node) {
			depth++
		}
		for _, child := range children(item.node) {
			stack = append(stack, surveyItem{node: child, depth: depth})
		}
	}
}

// declareAnchor records an anchor and refuses a name that is already taken. A
// second anchor of the same name is legal YAML, and it means an alias resolves
// to whichever one the reader passed most recently. That is a rule nobody
// should have to know to read a job file, so paceq refuses the ambiguity
// instead of picking a side.
func (d *decoder) declareAnchor(n *ast.AnchorNode) bool {
	name := anchorName(n)
	if name == "" {
		d.error(CodeTooManyAliases, position(n),
			"an anchor here has no name",
			"An anchor is written &name, and used later as *name.")
		return false
	}
	if _, taken := d.anchors[name]; taken {
		d.error(CodeTooManyAliases, position(n),
			fmt.Sprintf("the anchor &%s is defined twice", name),
			fmt.Sprintf("Give the second one another name. With two, *%s means whichever came last,\n", name)+
				"and that is not something a reader of the file can see.")
		return false
	}
	d.anchors[name] = n.Value
	return true
}

func anchorName(n *ast.AnchorNode) string {
	if n.Name == nil {
		return ""
	}
	return n.Name.String()
}

func isContainer(n ast.Node) bool {
	switch n.(type) {
	case *ast.MappingNode, *ast.SequenceNode:
		return true
	}
	return false
}

// children is every node reachable from one node without following an alias.
// An alias yields its name, never its target: that is what keeps the walk
// linear in the size of the file.
func children(n ast.Node) []ast.Node {
	switch v := n.(type) {
	case *ast.DocumentNode:
		return []ast.Node{v.Body}
	case *ast.MappingNode:
		out := make([]ast.Node, 0, len(v.Values))
		for _, value := range v.Values {
			out = append(out, value)
		}
		return out
	case *ast.MappingValueNode:
		return []ast.Node{v.Key, v.Value}
	case *ast.SequenceNode:
		return append([]ast.Node(nil), v.Values...)
	case *ast.AnchorNode:
		return []ast.Node{v.Value}
	case *ast.TagNode:
		return []ast.Node{v.Value}
	}
	return nil
}

// resolve follows aliases and unwraps anchors down to the node that carries the
// value, spending a unit of budget for every node it passes through. The budget
// is what nested aliases cannot get around: the tree limits bound how many
// aliases a file may contain, and this bounds how much they may add up to.
func (d *decoder) resolve(n ast.Node, where string) (ast.Node, bool) {
	for {
		if d.stopped {
			return nil, false
		}
		if d.budget <= 0 {
			d.error(CodeTooLarge, position(n),
				fmt.Sprintf("%s expands to more than paceq reads", where),
				fmt.Sprintf("The aliases in this file multiply out past %d nodes.\n", MaxExpandedNodes)+
					"Write the values out instead of nesting anchors inside anchors.")
			return nil, false
		}
		d.budget--

		switch v := n.(type) {
		case *ast.AnchorNode:
			n = v.Value
		case *ast.AliasNode:
			name := aliasName(v)
			target, ok := d.anchors[name]
			if !ok {
				d.error(CodeTooManyAliases, position(v),
					fmt.Sprintf("%s uses *%s, and no anchor &%s is defined", where, name, name),
					fmt.Sprintf("Define it before you use it:\n\n    shared: &%s\n      KEY: value\n", name)+
						fmt.Sprintf("\nAn anchor has to appear earlier in the file than the *%s that reads it.", name))
				return nil, false
			}
			if d.open[name] {
				d.error(CodeTooManyAliases, position(v),
					fmt.Sprintf("the anchor &%s contains *%s, which is itself", name, name),
					"Remove the alias from inside the anchor it points at. A value that contains\n"+
						"itself has no size, and paceq cannot write it into a job.")
				return nil, false
			}
			d.open[name] = true
			resolved, ok := d.resolve(target, where)
			delete(d.open, name)
			return resolved, ok
		default:
			return n, true
		}
	}
}

func aliasName(n *ast.AliasNode) string {
	if n.Value == nil {
		return ""
	}
	return n.Value.String()
}

// spend takes one unit of budget for a node the decoder is about to read. It is
// the same budget resolve uses, so a file that reaches its limit through plain
// nesting is refused on the same terms as one that reaches it through aliases.
func (d *decoder) spend(n ast.Node, where string) bool {
	if d.stopped {
		return false
	}
	if d.budget <= 0 {
		d.error(CodeTooLarge, position(n),
			fmt.Sprintf("%s expands to more than paceq reads", where),
			fmt.Sprintf("The file multiplies out past %d nodes.\n", MaxExpandedNodes)+
				"Write the values out instead of nesting anchors inside anchors.")
		return false
	}
	d.budget--
	return true
}
