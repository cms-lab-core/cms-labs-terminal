// Command cms-labs-terminal runs a namespace-local persistent terminal broker and ttyd frontends.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/maintainer64/cms-labs-terminal/internal/broker"
	"github.com/maintainer64/cms-labs-terminal/internal/config"
	"github.com/maintainer64/cms-labs-terminal/internal/kubeexec"
	"github.com/maintainer64/cms-labs-terminal/internal/loopback"
	"github.com/maintainer64/cms-labs-terminal/internal/planner"
	"github.com/maintainer64/cms-labs-terminal/internal/target"
)

const (
	defaultConfigPath   = "/etc/cms-labs-terminal/config.yaml"
	configPathEnv       = "CMS_LABS_TERMINAL_CONFIG"
	ttydPathEnv         = "CMS_LABS_TERMINAL_TTYD"
	kubeconfigEnv       = "KUBECONFIG"
	serviceAccountToken = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec // path, not a credential.
)

var version = "dev"

func main() {
	mode := "serve"
	arguments := os.Args[1:]
	if len(arguments) > 0 && (arguments[0] == "--version" || arguments[0] == "-version") {
		mode, arguments = arguments[0], arguments[1:]
	} else if len(arguments) > 0 && arguments[0] != "" && arguments[0][0] != '-' {
		mode, arguments = arguments[0], arguments[1:]
	}

	var err error
	switch mode {
	case "serve":
		err = serve(arguments)
	case "connect":
		err = connect(arguments)
	case "resolve":
		err = resolve(arguments)
	case "diagnose":
		err = diagnose()
	case "version", "--version", "-version":
		fmt.Printf("cms-labs-terminal %s\n", version)
		return
	default:
		err = fmt.Errorf("unknown mode %q (use serve, connect, resolve, diagnose or version)", mode)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "cms-labs-terminal: %s\n", err)
		os.Exit(1)
	}
}

func serve(arguments []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	configFile := flags.String("config", configPath(), "configuration file")
	verbose := flags.Bool("verbose", false, "debug logging")
	if err := flags.Parse(arguments); err != nil {
		return err
	}

	configuration, err := config.Load(*configFile)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	client, restConfiguration, err := clusterClient()
	if err != nil {
		return err
	}
	resolver := target.New(client, configuration.Access)
	targetPlanner := planner.New(configuration, resolver)
	registry := broker.NewRegistry(kubeexec.New(client, restConfiguration), targetPlanner.Plan, broker.Settings{
		Lease: configuration.Broker.Lease.Duration, ReapInterval: configuration.Broker.ReapInterval.Duration,
		ScrollbackBytes: configuration.Broker.ScrollbackBytes, History: configuration.Broker.History,
	})
	bridge := loopback.NewServer(configuration.Broker.Address, registry, logger)
	listener, err := bridge.Listen()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go registry.Run(ctx)

	errorsChannel := make(chan error, len(configuration.Targets)+1)
	go func() { errorsChannel <- bridge.Serve(listener) }()

	commands := make([]*exec.Cmd, 0, len(configuration.Targets))
	for _, terminalTarget := range configuration.Targets {
		command := exec.CommandContext(ctx, ttydPath(), ttydArguments(configuration, terminalTarget)...) //nolint:gosec // argv is validated config.
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		command.Cancel = func() error {
			if command.Process == nil {
				return nil
			}
			return command.Process.Signal(syscall.SIGTERM)
		}
		command.WaitDelay = 5 * time.Second
		if err = command.Start(); err != nil {
			stop()
			shutdownBridge(bridge)
			return fmt.Errorf("starting ttyd for %s: %w", terminalTarget.Name, err)
		}
		commands = append(commands, command)
		logger.Info("ttyd started", "target", terminalTarget.Name, "port", terminalTarget.Port, "pid", command.Process.Pid)
		go func(name string, process *exec.Cmd) {
			errorsChannel <- fmt.Errorf("ttyd for %s exited: %w", name, process.Wait())
		}(terminalTarget.Name, command)
	}

	var runError error
	select {
	case <-ctx.Done():
	case runError = <-errorsChannel:
		stop()
	}
	shutdownBridge(bridge)
	for _, command := range commands {
		if command.Process != nil {
			_ = command.Process.Signal(syscall.SIGTERM)
		}
	}
	if runError != nil && !errors.Is(runError, context.Canceled) {
		return runError
	}
	return nil
}

func ttydArguments(configuration config.Config, target config.Target) []string {
	arguments := []string{
		"--port", strconv.Itoa(target.Port),
		"--interface", configuration.Server.Address,
		"--writable",
		"--max-clients", strconv.Itoa(configuration.Server.MaxClients),
		"--client-option", "titleFixed=" + configuration.Server.Title + " — " + target.Label,
		"--client-option", "disableLeaveAlert=true",
	}
	if !configuration.Server.DisableOriginCheck {
		arguments = append(arguments, "--check-origin")
	}
	if !configuration.Server.AllowUnauthenticated {
		arguments = append(arguments, "--auth-header", configuration.Server.AuthHeader)
	}
	return append(arguments,
		selfPath(), "connect", "--broker", configuration.Broker.Address, "--target", target.Name,
	)
}

func connect(arguments []string) error {
	flags := flag.NewFlagSet("connect", flag.ContinueOnError)
	address := flags.String("broker", config.DefaultBrokerAddress, "loopback broker address")
	targetName := flags.String("target", "", "configured terminal target")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *targetName == "" {
		return fmt.Errorf("--target is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return (&loopback.Client{
		Address: *address, Target: *targetName, Identity: os.Getenv("TTYD_USER"),
		Input: os.Stdin, Output: os.Stdout,
	}).Run(ctx)
}

func resolve(arguments []string) error {
	flags := flag.NewFlagSet("resolve", flag.ContinueOnError)
	configFile := flags.String("config", configPath(), "configuration file")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("resolve expects exactly one target name")
	}
	configuration, err := config.Load(*configFile)
	if err != nil {
		return err
	}
	client, _, err := clusterClient()
	if err != nil {
		return err
	}
	resolved, err := planner.New(configuration, target.New(client, configuration.Access)).Plan(context.Background(), flags.Arg(0))
	if err != nil {
		return err
	}
	fmt.Printf("target: %s\nmode: %s\nnamespace: %s\npod: %s\ncontainer: %s\ncommand: %v\n",
		resolved.Target, resolved.Mode, resolved.Namespace, resolved.Pod, resolved.Container, resolved.Command)
	return nil
}

func diagnose() error {
	configuration, err := config.Load(configPath())
	if err != nil {
		return err
	}
	fmt.Printf("cms-labs-terminal %s\n", version)
	fmt.Printf("broker: %s (lease %s, scrollback %d bytes, history %d)\n",
		configuration.Broker.Address, configuration.Broker.Lease, configuration.Broker.ScrollbackBytes, configuration.Broker.History)
	for _, terminalTarget := range configuration.Targets {
		fmt.Printf("target: %s port=%d mode=%s command=%v\n", terminalTarget.Name, terminalTarget.Port, terminalTarget.Mode, terminalTarget.Command)
	}
	return nil
}

func clusterClient() (kubernetes.Interface, *rest.Config, error) {
	restConfiguration, err := restConfig()
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(restConfiguration)
	if err != nil {
		return nil, nil, fmt.Errorf("building kubernetes client: %w", err)
	}
	return client, restConfiguration, nil
}

func restConfig() (*rest.Config, error) {
	if _, err := os.Stat(serviceAccountToken); err == nil {
		configuration, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("reading in-cluster credentials: %w", err)
		}
		return configuration, nil
	}
	path := os.Getenv(kubeconfigEnv)
	if path == "" {
		return nil, fmt.Errorf("no in-cluster credentials and %s is unset", kubeconfigEnv)
	}
	configuration, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	return configuration, nil
}

func shutdownBridge(server *loopback.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func configPath() string {
	if path := os.Getenv(configPathEnv); path != "" {
		return path
	}
	return defaultConfigPath
}

func ttydPath() string {
	if path := os.Getenv(ttydPathEnv); path != "" {
		return path
	}
	return "/usr/bin/ttyd"
}

func selfPath() string {
	path, err := os.Executable()
	if err == nil {
		return path
	}
	return "/cms-labs-terminal"
}
