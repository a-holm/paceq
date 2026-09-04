// Package spec is the job definition: YAML in, canonical JSON out.
//
// # Why there is an IR at all
//
// The engine never reads YAML. It reads a canonical JSON document called
// paceq.job.v1, carrying a spec_hash, and this package is the only thing that
// produces one (03 section 3.2, SYNTESE section 3.3). That buys three things
// for the price of one encoder. A second frontend is a second frontend, not a
// second engine. Versioning a job is comparing two hashes rather than diffing
// two files. And "show me exactly what ran" stays true after the file on disk
// has been edited, because the run stored the hash of what it ran.
//
// The rule that makes the hash worth anything is that defaults are materialised
// before hashing. A job that writes max_concurrent: 1 and a job that leaves it
// out are the same job, and they must hash the same, or every apply after a
// tidy-up of the file would record a version that changed nothing.
//
// # Why the decoder is written out by hand
//
// Parse walks the YAML syntax tree itself rather than unmarshalling into the
// structs. Three things fall out of that which reflection does not give:
//
//   - Every diagnostic carries the line and column of the thing it is about,
//     which is what makes the excerpt and the caret possible.
//   - An unknown field is caught with the name the user typed still in hand, so
//     the message can say what they probably meant.
//   - Scalars are read as the text that is in the file. YAML 1.1 turns the
//     country code NO into false, and a job that inherits an environment
//     variable called NO deserves better than a silent boolean.
//
// # Limits before decoding
//
// A YAML file that expands to more than it says is the reason for every limit
// in this package (08 T13). The syntax tree is bounded work: an alias in it is
// a name, not a copy. So the file size, the nesting depth, the alias count and
// the node count are all checked on the tree, before anything is expanded, and
// the decoder that does expand aliases carries a budget it cannot exceed. A
// billion laughs file is refused having allocated a syntax tree the size of the
// file it came from.
//
// # What is deliberately not here
//
// No templating, in any form (SYNTESE section 3.3). No execution semantics:
// needs is validated here, cycle check included, but the engine is what gates
// a step on its edges. Schedules and sensors validate here and fire from the
// scheduler and the sensor runtime. Nothing in this package reads a database
// or starts a process.
package spec
