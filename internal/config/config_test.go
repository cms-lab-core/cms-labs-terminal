package config

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		Access:  Access{Namespaces: []string{"lab-attempt"}},
		Targets: []Target{{Name: "r1", Port: 7681}},
	}
}

func TestValidateAppliesSafeDefaults(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate: %s", err)
	}
	if configuration.Server.AuthHeader != DefaultAuthHeader {
		t.Fatalf("auth header = %q, want %q", configuration.Server.AuthHeader, DefaultAuthHeader)
	}
	if configuration.Broker.Address != DefaultBrokerAddress {
		t.Fatalf("broker = %q, want loopback default", configuration.Broker.Address)
	}
	if configuration.Broker.Lease.Duration != 30*time.Minute {
		t.Fatalf("lease = %s, want 30m", configuration.Broker.Lease.Duration)
	}
	if configuration.Targets[0].Mode != string(ModeExec) {
		t.Fatalf("mode = %q, want exec", configuration.Targets[0].Mode)
	}
	if !slices.Equal(configuration.Targets[0].Command, []string{DefaultCommand}) {
		t.Fatalf("command = %v, want default shell", configuration.Targets[0].Command)
	}
}

func TestAttachHasNoCommand(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	configuration.Session.Mode = string(ModeAttach)
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate: %s", err)
	}
	if len(configuration.Targets[0].Command) != 0 {
		t.Fatalf("attach command = %v, want none", configuration.Targets[0].Command)
	}
}

func TestAttachRejectsACommand(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	configuration.Targets[0].Mode = string(ModeAttach)
	configuration.Targets[0].Command = []string{"bash"}
	if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestAttachSessionDefaultRejectsACommand(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	configuration.Session.Mode = string(ModeAttach)
	configuration.Session.Command = []string{"bash"}
	if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestTargetsOverrideSessionCommandIndependently(t *testing.T) {
	t.Parallel()
	configuration := Config{
		Access:  Access{Namespaces: []string{"lab-attempt"}},
		Session: Session{Mode: string(ModeExec), Command: []string{"/bin/sh"}},
		Targets: []Target{
			{Name: "srl1", Port: 7681, Command: []string{"sr_cli"}},
			{Name: "pc1", Port: 7682, Command: []string{"/bin/bash", "-l"}},
			{Name: "alpine1", Port: 7683},
		},
	}
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate: %s", err)
	}
	want := [][]string{{"sr_cli"}, {"/bin/bash", "-l"}, {"/bin/sh"}}
	for index := range configuration.Targets {
		if !slices.Equal(configuration.Targets[index].Command, want[index]) {
			t.Fatalf("target %q command = %v, want %v", configuration.Targets[index].Name, configuration.Targets[index].Command, want[index])
		}
	}
}

func TestNamespaceLocalDeploymentRejectsWildcardAccess(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	configuration.Access.Namespaces = []string{"*"}
	if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestTargetCannotEscapeAllowedNamespace(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	configuration.Targets[0].Namespace = "kube-system"
	if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestNamesAndPortsAreUnique(t *testing.T) {
	t.Parallel()
	for name, targets := range map[string][]Target{
		"name": {{Name: "r1", Port: 7681}, {Name: "r1", Port: 7682}},
		"port": {{Name: "r1", Port: 7681}, {Name: "r2", Port: 7681}},
	} {
		t.Run(name, func(t *testing.T) {
			configuration := validConfig()
			configuration.Targets = targets
			if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestBrokerMustStayOnLoopback(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	configuration.Broker.Address = "0.0.0.0:17680"
	if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestTargetCannotBindTheLoopbackBrokerPort(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	configuration.Broker.Address = "127.0.0.1:7681"
	if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestLoadParsesDurationsAndRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "config.yaml")
	content := `
server:
  unknownSetting: true
broker:
  lease: 45m
access:
  namespaces: [lab]
targets:
  - name: r1
    port: 7681
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "unknownSetting") {
		t.Fatalf("Load error = %v, want unknown field named", err)
	}

	content = strings.Replace(content, "  unknownSetting: true\n", "", 1)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %s", err)
	}
	if configuration.Broker.Lease.Duration != 45*time.Minute {
		t.Fatalf("lease = %s, want 45m", configuration.Broker.Lease.Duration)
	}
}

func TestTargetReturnsOnlyDeclaredTargets(t *testing.T) {
	t.Parallel()
	configuration := validConfig()
	if target, ok := configuration.Target("r1"); !ok || target.Name != "r1" {
		t.Fatalf("Target(r1) = %+v, %t", target, ok)
	}
	if _, ok := configuration.Target("kube-system"); ok {
		t.Fatal("undeclared target was accepted")
	}
}
