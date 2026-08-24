package spec

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/a-holm/paceq/internal/diag"
)

// Extensions are the file names a directory walk picks up as job files.
var Extensions = []string{".yaml", ".yml"}

// Source is one job file as it was read, kept so a diagnostic can be rendered
// with the excerpt it points at after the file is closed.
type Source struct {
	Path  string
	Bytes []byte
}

// ReadFile reads one job file without ever holding more than the limit in
// memory. That is what makes the size refusal real: a two megabyte file is
// refused having read one megabyte and one byte of it, and the YAML parser
// never sees it.
func ReadFile(path string) (Source, diag.List) {
	file, err := os.Open(path) // #nosec G304 -- the path is what the operator asked paceq to validate
	if err != nil {
		return Source{Path: path}, diag.List{readFailure(path, err)}
	}
	defer func() { _ = file.Close() }()

	// One byte past the limit is enough to know the file is over it.
	src, err := io.ReadAll(io.LimitReader(file, MaxFileBytes+1))
	if err != nil {
		return Source{Path: path}, diag.List{readFailure(path, err)}
	}
	return Source{Path: path, Bytes: src}, nil
}

func readFailure(path string, err error) diag.Diagnostic {
	hint := "Check the path and the permissions on it."
	switch {
	case errors.Is(err, fs.ErrNotExist):
		hint = "Check the path. paceq reads job files from the jobs directory by default:\n\n" +
			"    paceq validate jobs/"
	case errors.Is(err, fs.ErrPermission):
		hint = "paceq is running as a user that cannot read the file:\n\n" +
			"    ls -l " + path
	}
	return diag.New(CodeSyntax, diag.SeverityError, path, diag.Position{},
		fmt.Sprintf("the job file cannot be read: %v", err), hint)
}

// LoadFile reads and parses one job file.
func LoadFile(path string) (*Job, Source, diag.List) {
	source, diags := ReadFile(path)
	if len(diags) > 0 {
		return nil, source, diags
	}
	job, diags := Parse(path, source.Bytes)
	return job, source, diags
}

// Collect turns what was typed on a command line into the list of job files to
// read. A path that names a file is taken as it is, whatever it is called; a
// path that names a directory is walked for the job file extensions.
//
// The result is sorted, so two runs over the same tree report in the same
// order and a golden test can compare them.
func Collect(paths []string) ([]string, error) {
	var files []string
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			files = append(files, filepath.Clean(path))
			continue
		}
		found, err := walkJobFiles(path)
		if err != nil {
			return nil, err
		}
		files = append(files, found...)
	}

	sort.Strings(files)
	return dedupe(files), nil
}

func walkJobFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// A dot directory in a jobs tree is somebody's editor or version
			// control, never a job.
			if path != dir && strings.HasPrefix(entry.Name(), ".") {
				return fs.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			// A link inside the jobs tree can name anything on disk (08 T11),
			// and a pipe or a socket is nobody's job file. Neither is read,
			// and a link to a directory is never walked into.
			return nil
		}
		if IsJobFile(entry.Name()) {
			files = append(files, filepath.Clean(path))
		}
		return nil
	})
	return files, err
}

// IsJobFile reports whether a directory walk picks a name up.
func IsJobFile(name string) bool {
	if strings.HasPrefix(name, ".") {
		return false
	}
	extension := strings.ToLower(filepath.Ext(name))
	return contains(Extensions, extension)
}

func dedupe(sorted []string) []string {
	out := sorted[:0]
	previous := ""
	for i, path := range sorted {
		if i > 0 && path == previous {
			continue
		}
		out = append(out, path)
		previous = path
	}
	return out
}

// NamedJob is one parsed job and the path it came from, the input the checks
// that need the whole catalog run over.
type NamedJob struct {
	Path string
	Job  *Job
}

// CheckGlobalSensorNames rejects two jobs that define the same sensor name. A
// sensor name is the sensor row's primary key across every job, so two owners
// of one name cannot both materialise; the later apply would silently steal
// the row from the first. Each diagnostic points at one conflicting job and
// names the file that already owns the name.
func CheckGlobalSensorNames(jobs []NamedJob) diag.List {
	var out diag.List
	owner := map[string]string{}
	for _, named := range jobs {
		for _, sensor := range named.Job.Sensors {
			if sensor.Name == "" {
				continue
			}
			if first, taken := owner[sensor.Name]; taken {
				out = append(out, diag.New(CodeSensorNameTaken, diag.SeverityError, named.Path, diag.Position{},
					"the sensor "+sensor.Name+" is already used by the job in "+first,
					"A sensor name is the primary key its row lives under across every job,\n"+
						"so two jobs can never both own "+sensor.Name+". The later apply would\n"+
						"silently move the row from the first job to the second:\n\n"+
						"    "+first+"          already owns it\n"+
						"    "+named.Path+"   is the job trying to take it\n\n"+
						"Rename the sensor in one of the files."))
				continue
			}
			owner[sensor.Name] = named.Path
		}
	}
	return out
}
