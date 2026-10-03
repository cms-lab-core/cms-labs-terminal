// Package planner resolves a declared terminal target to one running Kubernetes container.
package planner

import (
	"context"
	"fmt"

	"github.com/maintainer64/cms-labs-terminal/internal/broker"
	"github.com/maintainer64/cms-labs-terminal/internal/config"
	"github.com/maintainer64/cms-labs-terminal/internal/target"
)

type Planner struct {
	configuration config.Config
	resolver      *target.Resolver
}

func New(configuration config.Config, resolver *target.Resolver) *Planner {
	return &Planner{configuration: configuration, resolver: resolver}
}

func (p *Planner) Plan(ctx context.Context, name string) (broker.Spec, error) {
	configured, ok := p.configuration.Target(name)
	if !ok {
		return broker.Spec{}, fmt.Errorf("terminal target %q is not declared", name)
	}

	resolved, err := p.resolver.Resolve(
		ctx, configured.Name, configured.Pod, configured.Namespace, configured.Container,
	)
	if err != nil {
		return broker.Spec{}, err
	}

	return broker.Spec{
		Target: configured.Name, Label: configured.Label,
		Mode: broker.Mode(configured.Mode), Command: append([]string(nil), configured.Command...),
		Namespace: resolved.Namespace, Pod: resolved.Pod, Container: resolved.Container,
	}, nil
}
