package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/goccy/go-yaml"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/notify"
)

// NotifierFileName is the configuration file that names notifiers and
// defaults. It lives in the state directory first (portable per project) and
// falls back to the system configuration directory (/etc/paceq).
const NotifierFileName = "config.yaml"

// DefaultNotifierConfigDir is where a system-wide configuration looks when the
// state directory carries none.
const DefaultNotifierConfigDir = "/etc/paceq"

var notifierNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// EnvNamePattern is what an inherited variable name must look like - the same
// grammar execve accepts, minus anything containing '=' or a newline.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// notifierFileEntry is one configured delivery target exactly as written:
// one type, and for exec one absolute argv. There is no shell string and no
// templating ever (SYNTESE section 3.7): arguments are static, the event
// arrives as JSON on stdin plus PULSEQ_* variables.
type notifierFileEntry struct {
	Type       string   `yaml:"type"`
	Run        []string `yaml:"run"`
	Timeout    string   `yaml:"timeout"`
	InheritEnv []string `yaml:"inherit_env"`
}

// NotificationConfig is the whole [notifiers + defaults] document.
type NotificationConfig struct {
	Notifiers map[string]*notify.ExecNotifier // type=exec targets
	Stderr    []string                        // names bound to stderr output

	Entries  map[string]notifierFileEntry
	Defaults model.NotifyDefaults

	// Timeouts mirrors Entries keyed by name, parsed and defaulted.
	Timeouts map[string]time.Duration
}

// notifierDoc mirrors config.yaml on disk.
type notifierDoc struct {
	Notifiers      map[string]notifierFileEntry `yaml:"notifiers"`
	NotifyDefaults struct {
		OnFailure   []string `yaml:"on_failure"`
		OnSuccess   []string `yaml:"on_success"`
		Throttle    string   `yaml:"throttle"`
		GroupBy     []string `yaml:"group_by"`
		MaxAttempts int      `yaml:"max_attempts"` // zero means unset here
	} `yaml:"notify_defaults"`
}

// LoadNotificationConfig reads the notification configuration from stateDir/config.yaml
// or, failing that, configDir/config.yaml. A missing file everywhere is NOT
// an error: it means notifications are simply not configured, which is every
// installation's honest starting point. Bad syntax or bad values refuse loudly,
// because a silently dropped alert is the exact failure this milestone exists
// to prevent.
func LoadNotificationConfig(stateDir, configDir string) (*NotificationConfig, error) {
	candidates := []string{}
	if stateDir != "" {
		candidates = append(candidates, filepath.Join(stateDir, NotifierFileName))
	}
	if configDir != "" {
		candidates = append(candidates, filepath.Join(configDir, NotifierFileName))
	}
	for _, path := range candidates {
		data, err := os.ReadFile(path) // #nosec G304 - the path is filepath.Join of paceq-computed directories with the fixed NotifierFileName constant, never job or remote input
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		cfg, perr := parseNotificationConfig(data)
		if perr != nil {
			return nil, fmt.Errorf("%s: %w", path, perr)
		}
		return cfg, nil
	}
	return nil, nil
}

// parseNotificationConfig validates every key the moment it is read: a typo'd
// timeout unit refuses here rather than becomes a failed delivery at 03:00.
func parseNotificationConfig(data []byte) (*NotificationConfig, error) {
	var doc notifierDoc
	if err := yaml.UnmarshalWithOptions(data, &doc, yaml.Strict()); err != nil {
		return nil, fmt.Errorf("parse the configuration: %w", err)
	}
	cfg := &NotificationConfig{
		Notifiers: map[string]*notify.ExecNotifier{},
		Stderr:    nil,
		Entries:   map[string]notifierFileEntry{},
		Defaults: model.NotifyDefaults{
			Throttle: defaultThrottle(),
		},
		Timeouts: map[string]time.Duration{},
	}
	if len(doc.Notifiers) == 0 && (len(doc.NotifyDefaults.OnFailure) > 0 || len(doc.NotifyDefaults.OnSuccess) > 0) {
		return nil, fmt.Errorf("notify_defaults names targets but no notifiers are defined")
	}
	for name, entry := range doc.Notifiers {
		if !notifierNamePattern.MatchString(name) {
			return nil, fmt.Errorf("notifier %q: names are lower case letters, digits, underscores and dashes", name)
		}
		switch entry.Type {
		case "exec":
			if len(entry.Run) == 0 || entry.Run[0] == "" {
				return nil, fmt.Errorf("notifier %q of type exec needs a run list whose first item is the command", name)
			}
			if !filepath.IsAbs(entry.Run[0]) {
				return nil, fmt.Errorf("notifier %q runs %q, want an absolute command: its environment starts empty, so there is no PATH to search", name, entry.Run[0])
			}
			for _, kv := range entry.InheritEnv {
				if !envNamePattern.MatchString(kv) {
					return nil, fmt.Errorf("notifier %q inherits %q, which is not a name an environment variable can carry", name, kv)
				}
			}
			timeout, terr := notifierTimeout(entry.Timeout)
			if terr != nil {
				return nil, fmt.Errorf("notifier %q: %w", name, terr)
			}
			cfg.Entries[name] = entry
			cfg.Timeouts[name] = timeout
			cfg.Notifiers[name] = &notify.ExecNotifier{
				Name:       name,
				Argv:       append([]string(nil), entry.Run...),
				InheritEnv: append([]string(nil), entry.InheritEnv...),
			}
		case "stderr":
			cfg.Entries[name] = entry
			cfg.Stderr = append(cfg.Stderr, name)
		default:
			return nil, fmt.Errorf("notifier %q has type %q, want exec or stderr", name, entry.Type)
		}
	}

	resolve := func(names []string) error {
		for _, n := range names {
			if _, ok := cfg.Entries[n]; !ok {
				return fmt.Errorf("default names notifier %q, which does not exist", n)
			}
		}
		return nil
	}
	if err := resolve(doc.NotifyDefaults.OnFailure); err != nil {
		return nil, err
	}
	if err := resolve(doc.NotifyDefaults.OnSuccess); err != nil {
		return nil, err
	}
	cfg.Defaults.OnFailure = doc.NotifyDefaults.OnFailure
	cfg.Defaults.OnSuccess = doc.NotifyDefaults.OnSuccess
	if t := strings.TrimSpace(doc.NotifyDefaults.Throttle); t != "" {
		d, err := time.ParseDuration(t)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("throttle %q is not a duration like 15m", doc.NotifyDefaults.Throttle)
		}
		cfg.Defaults.Throttle = d
	}
	for _, f := range doc.NotifyDefaults.GroupBy {
		if !notify.ValidGroupField(f) {
			return nil, fmt.Errorf("group_by %q is unknown; accepted fields: job, reason_code", f)
		}
	}
	cfg.Defaults.GroupBy = doc.NotifyDefaults.GroupBy
	if doc.NotifyDefaults.MaxAttempts != 0 {
		n := doc.NotifyDefaults.MaxAttempts
		if n < 1 || n > 100 {
			return nil, fmt.Errorf("max_attempts is %d, want 1..100", n)
		}
		cfg.Defaults.MaxAttempts = n
	}
	return cfg, nil
}

func notifierTimeout(raw string) (time.Duration, error) {
	if raw == "" {
		return notify.DefaultDeliveryTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("timeout %q is not a positive duration like 30s", raw)
	}
	if d > 10*time.Minute {
		return 0, fmt.Errorf("timeout %s exceeds the ten minute ceiling", d)
	}
	return d, nil
}

// defaultThrottle mirrors the issue sketch's fifteen minutes: enough to
// collapse a retry storm, short enough that a real recovery notice waits no
// more than a coffee break.
func defaultThrottle() time.Duration { return 15 * time.Minute }

// HostName captures the machine identity stamped into payloads once, so a
// churning hostname mid-process cannot make two alerts disagree about where
// they came from. Empty stays empty in the payload: the field documents the
// machine when it knows its own name.
func HostName() string {
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return host
}
