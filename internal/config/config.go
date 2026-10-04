// Package config defines the namespace-local terminal deployment contract.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	DefaultAddress             = "0.0.0.0"
	DefaultBrokerAddress       = "127.0.0.1:17680"
	DefaultAuthHeader          = "X-CMS-Identity"
	DefaultCommand             = "/bin/sh"
	DefaultNodeLabel           = "c9s.run/topologyNode"
	DefaultOwnerLabel          = "c9s.run/topologyOwner"
	DefaultContainerAnnotation = "kubectl.kubernetes.io/default-container"
	DefaultMaxClients          = 8
	DefaultScrollbackBytes     = 1 << 20
	DefaultHistory             = 3
	maxScrollbackBytes         = 64 << 20
)

var ErrInvalid = errors.New("invalid terminal configuration")

type Config struct {
	Server  Server   `yaml:"server"`
	Broker  Broker   `yaml:"broker"`
	Access  Access   `yaml:"access"`
	Session Session  `yaml:"session"`
	Targets []Target `yaml:"targets"`
}

type Server struct {
	Address string `yaml:"address"`
	Title   string `yaml:"title"`
	// AuthHeader is populated by the trusted Clabgate/nginx auth_request path. ttyd validates its
	// presence and exports the value as TTYD_USER to the loopback client.
	AuthHeader string `yaml:"authHeader"`
	// AllowUnauthenticated is for a developer's port-forward only. Production manifests leave it off.
	AllowUnauthenticated bool `yaml:"allowUnauthenticated"`
	MaxClients           int  `yaml:"maxClients"`
	DisableOriginCheck   bool `yaml:"disableOriginCheck"`
}

type Broker struct {
	Address         string   `yaml:"address"`
	Lease           Duration `yaml:"lease"`
	ReapInterval    Duration `yaml:"reapInterval"`
	ScrollbackBytes int      `yaml:"scrollbackBytes"`
	History         int      `yaml:"history"`
}

type Access struct {
	Namespaces []string `yaml:"namespaces"`
	NodeLabel  string   `yaml:"nodeLabel"`
	OwnerLabel string   `yaml:"ownerLabel"`
	Owner      string   `yaml:"owner"`
}

// Session supplies defaults. A target may override mode and command independently.
type Session struct {
	Mode    string   `yaml:"mode"`
	Command []string `yaml:"command"`
}

// Target is one browser endpoint and one long-lived remote PTY.
type Target struct {
	Name      string   `yaml:"name"`
	Label     string   `yaml:"label"`
	Port      int      `yaml:"port"`
	Namespace string   `yaml:"namespace"`
	Pod       string   `yaml:"pod"`
	Container string   `yaml:"container"`
	Mode      string   `yaml:"mode"`
	Command   []string `yaml:"command"`
}

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // configured by the operator.
	if err != nil {
		return Config{}, fmt.Errorf("reading %s: %w", path, err)
	}
	configuration, err := Decode(raw)
	if err != nil {
		return Config{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if err = configuration.Validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return configuration, nil
}

// Decode parses a strict terminal configuration without applying runtime-specific defaults. It is
// used by the cluster controller, which injects the owning namespace and listener ports before
// calling Validate.
func Decode(raw []byte) (Config, error) {
	var configuration Config
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&configuration); err != nil {
		return Config{}, err
	}
	return configuration, nil
}

func (c *Config) Validate() error {
	if c.Server.Address == "" {
		c.Server.Address = DefaultAddress
	}
	if c.Server.Title == "" {
		c.Server.Title = "lab terminal"
	}
	if c.Server.AuthHeader == "" {
		c.Server.AuthHeader = DefaultAuthHeader
	}
	if c.Server.MaxClients <= 0 {
		c.Server.MaxClients = DefaultMaxClients
	}
	if c.Server.MaxClients > 64 {
		return fmt.Errorf("%w: server.maxClients cannot exceed 64", ErrInvalid)
	}

	if c.Broker.Address == "" {
		c.Broker.Address = DefaultBrokerAddress
	}
	if err := validateLoopback(c.Broker.Address); err != nil {
		return err
	}
	if c.Broker.Lease.Duration <= 0 {
		c.Broker.Lease.Duration = 30 * time.Minute
	}
	if c.Broker.Lease.Duration > 24*time.Hour {
		return fmt.Errorf("%w: broker.lease cannot exceed 24h", ErrInvalid)
	}
	if c.Broker.ReapInterval.Duration <= 0 {
		c.Broker.ReapInterval.Duration = time.Minute
	}
	if c.Broker.ScrollbackBytes <= 0 {
		c.Broker.ScrollbackBytes = DefaultScrollbackBytes
	}
	if c.Broker.ScrollbackBytes > maxScrollbackBytes {
		return fmt.Errorf("%w: broker.scrollbackBytes cannot exceed %d", ErrInvalid, maxScrollbackBytes)
	}
	if c.Broker.History <= 0 {
		c.Broker.History = DefaultHistory
	}
	if c.Broker.History > 100 {
		return fmt.Errorf("%w: broker.history cannot exceed 100", ErrInvalid)
	}

	if len(c.Access.Namespaces) == 0 {
		return fmt.Errorf("%w: access.namespaces must name this lab namespace", ErrInvalid)
	}
	if slices.Contains(c.Access.Namespaces, "*") {
		return fmt.Errorf("%w: access.namespaces cannot contain * in a namespace-local deployment", ErrInvalid)
	}
	if c.Access.NodeLabel == "" {
		c.Access.NodeLabel = DefaultNodeLabel
	}
	if c.Access.OwnerLabel == "" && c.Access.Owner != "" {
		c.Access.OwnerLabel = DefaultOwnerLabel
	}

	if c.Session.Mode == "" {
		c.Session.Mode = string(ModeExec)
	}
	if err := validateMode(c.Session.Mode); err != nil {
		return fmt.Errorf("%w: session.mode: %w", ErrInvalid, err)
	}
	if c.Session.Mode == string(ModeExec) && len(c.Session.Command) == 0 {
		c.Session.Command = []string{DefaultCommand}
	}
	if c.Session.Mode == string(ModeAttach) && len(c.Session.Command) > 0 {
		return fmt.Errorf("%w: attach session cannot specify a command", ErrInvalid)
	}

	if len(c.Targets) == 0 {
		return fmt.Errorf("%w: at least one target is required", ErrInvalid)
	}
	if len(c.Targets) > 64 {
		return fmt.Errorf("%w: targets cannot contain more than 64 entries", ErrInvalid)
	}
	names := make(map[string]struct{}, len(c.Targets))
	ports := make(map[int]string, len(c.Targets))
	_, brokerPortText, _ := net.SplitHostPort(c.Broker.Address)
	brokerPort, _ := strconv.Atoi(brokerPortText)
	for index := range c.Targets {
		target := &c.Targets[index]
		if strings.TrimSpace(target.Name) == "" {
			return fmt.Errorf("%w: targets[%d] has no name", ErrInvalid, index)
		}
		if problems := validation.IsDNS1123Label(target.Name); len(problems) > 0 || len(target.Name) > 54 {
			return fmt.Errorf(
				"%w: target name %q must be a DNS label no longer than 54 characters: %s",
				ErrInvalid, target.Name, strings.Join(problems, ", "),
			)
		}
		if _, exists := names[target.Name]; exists {
			return fmt.Errorf("%w: target name %q is repeated", ErrInvalid, target.Name)
		}
		names[target.Name] = struct{}{}

		if target.Port < 1 || target.Port > 65535 {
			return fmt.Errorf("%w: target %q port %d is invalid", ErrInvalid, target.Name, target.Port)
		}
		if target.Port == brokerPort {
			return fmt.Errorf("%w: target %q uses the broker port %d", ErrInvalid, target.Name, target.Port)
		}
		if previous, exists := ports[target.Port]; exists {
			return fmt.Errorf("%w: targets %q and %q both use port %d", ErrInvalid, previous, target.Name, target.Port)
		}
		ports[target.Port] = target.Name

		if target.Mode == "" {
			target.Mode = c.Session.Mode
		}
		if err := validateMode(target.Mode); err != nil {
			return fmt.Errorf("%w: target %q mode: %w", ErrInvalid, target.Name, err)
		}
		if target.Mode == string(ModeExec) && len(target.Command) == 0 {
			target.Command = append([]string(nil), c.Session.Command...)
		}
		if target.Mode == string(ModeExec) && len(target.Command) == 0 {
			return fmt.Errorf("%w: exec target %q has no command", ErrInvalid, target.Name)
		}
		if target.Mode == string(ModeAttach) && len(target.Command) > 0 {
			return fmt.Errorf("%w: attach target %q cannot specify a command", ErrInvalid, target.Name)
		}
		if target.Namespace != "" && !slices.Contains(c.Access.Namespaces, target.Namespace) {
			return fmt.Errorf("%w: target %q namespace %q is outside access.namespaces", ErrInvalid, target.Name, target.Namespace)
		}
		if target.Label == "" {
			target.Label = target.Name
		}
	}
	return nil
}

type Mode string

const (
	ModeExec   Mode = "exec"
	ModeAttach Mode = "attach"
)

func validateMode(mode string) error {
	switch Mode(mode) {
	case ModeExec, ModeAttach:
		return nil
	default:
		return fmt.Errorf("must be %q or %q, got %q", ModeExec, ModeAttach, mode)
	}
}

func validateLoopback(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: broker.address %q: %w", ErrInvalid, address, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%w: broker.address must be loopback, got %q", ErrInvalid, address)
	}
	return nil
}

func (c Config) Target(name string) (Target, bool) {
	for _, target := range c.Targets {
		if target.Name == name {
			return target, true
		}
	}
	return Target{}, false
}
