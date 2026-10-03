package main

import (
	"slices"
	"testing"

	"github.com/maintainer64/cms-labs-terminal/internal/config"
)

func TestTTYDArgumentsPinOneTargetAndRequireTrustedIdentity(t *testing.T) {
	t.Parallel()
	configuration := config.Config{
		Server: config.Server{
			Address: "0.0.0.0", Title: "Network lab", AuthHeader: "X-CMS-Identity", MaxClients: 4,
		},
		Broker: config.Broker{Address: "127.0.0.1:17680"},
	}
	target := config.Target{Name: "r1", Label: "Router 1", Port: 7681}
	arguments := ttydArguments(configuration, target)

	for _, required := range [][]string{
		{"--port", "7681"},
		{"--auth-header", "X-CMS-Identity"},
		{"--check-origin"},
		{"connect", "--broker", "127.0.0.1:17680", "--target", "r1"},
	} {
		if !containsSequence(arguments, required) {
			t.Errorf("arguments %v do not contain %v", arguments, required)
		}
	}
	if slices.Contains(arguments, "--url-arg") {
		t.Fatal("browser-controlled URL arguments could change the configured target")
	}
}

func TestTTYDArgumentsAllowExplicitLocalDevelopmentMode(t *testing.T) {
	t.Parallel()
	configuration := config.Config{
		Server: config.Server{Address: "127.0.0.1", AllowUnauthenticated: true, MaxClients: 1},
		Broker: config.Broker{Address: "127.0.0.1:17680"},
	}
	arguments := ttydArguments(configuration, config.Target{Name: "r1", Port: 7681})
	if slices.Contains(arguments, "--auth-header") {
		t.Fatalf("local development arguments unexpectedly require proxy auth: %v", arguments)
	}
}

func containsSequence(arguments, sequence []string) bool {
	if len(sequence) == 0 {
		return true
	}
	for index := 0; index+len(sequence) <= len(arguments); index++ {
		if slices.Equal(arguments[index:index+len(sequence)], sequence) {
			return true
		}
	}
	return false
}
