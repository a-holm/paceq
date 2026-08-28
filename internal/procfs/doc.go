// Package procfs is the one reader of the kernel's process facts. Both the
// runner (before it signals a process group) and the store (when it records a
// spawned attempt's identity) need field 22 of /proc/<pid>/stat, and issue
// #39 needs the boot id beside it, so the parser lives here where neither
// package has to own a second copy of the subtle part: the command name in
// field 2 may contain spaces and parentheses, so the numeric fields only
// begin after the last ')'.
//
// Everything here degrades on purpose off Linux: the stub answers "no
// answer", which every caller already treats as "refuse to act".
package procfs
