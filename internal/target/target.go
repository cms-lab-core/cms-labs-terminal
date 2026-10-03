// Package target resolves a stable node name to the pod and container a session should exec into.
//
// Pod names are generated and change every time a device Pod is recreated, so nothing may be keyed
// on them. Clabernetes labels each device Pod with the node's name from the Topology
// (c9s.run/topologyNode) and records the device container in an annotation, which is what makes a
// student's link survive redeployment.
package target

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/maintainer64/cms-labs-terminal/internal/config"
)

// ErrNotFound classifies a name that matches no running device pod. It is reported separately from
// a lookup error because the two need different advice: one means the link is stale, the other
// means the terminal cannot talk to the cluster.
var ErrNotFound = errors.New("no running device pod for this target")

// Resolved is a pod and container that exist right now.
type Resolved struct {
	Namespace string
	Pod       string
	Container string
	// UID identifies the pod incarnation, not just its name, so callers can distinguish a replacement
	// pod from the process they previously held.
	UID types.UID
}

// Describe renders the coordinates for an error message.
func (r Resolved) Describe() string {
	return fmt.Sprintf("%s/%s container %s", r.Namespace, r.Pod, r.Container)
}

// Resolver finds device pods.
type Resolver struct {
	client kubernetes.Interface
	access config.Access
}

// New returns a resolver bound to the configuration's access rules.
func New(client kubernetes.Interface, access config.Access) *Resolver {
	return &Resolver{client: client, access: access}
}

// Resolve finds the running pod for a node name, a pod name, or a configured target.
//
// The lookup order is deliberate. A pinned pod name is honoured exactly, because a lab that pinned
// one wants exactly that pod even while an older generation is still terminating. Otherwise the name
// is matched against the node label, and the newest match wins so a rollout's replacement is
// preferred over the pod it is replacing.
func (r *Resolver) Resolve(
	ctx context.Context,
	name string,
	pinnedPod string,
	pinnedNamespace string,
	pinnedContainer string,
) (Resolved, error) {
	if pinnedPod != "" {
		return r.resolvePinned(ctx, pinnedPod, pinnedNamespace, pinnedContainer)
	}

	if name == "" {
		return Resolved{}, fmt.Errorf(
			"%w: no node was named; open the link your lab gave you", ErrNotFound,
		)
	}

	candidates, err := r.findByNodeLabel(ctx, name)
	if err != nil {
		return Resolved{}, err
	}

	if len(candidates) == 0 {
		return Resolved{}, fmt.Errorf(
			"%w: nothing is labelled %s=%s in %s; the device may still be booting",
			ErrNotFound, r.access.NodeLabel, name, r.describeNamespaces(),
		)
	}

	// Candidates arrive newest first and are walked in that order, skipping any that cannot actually
	// be exec'd into. A rollout leaves the outgoing pod present but unusable for a few seconds, and
	// a student arriving in that window should still get the new one rather than an error.
	var rejected []string

	for _, pod := range candidates {
		container, declared, containerErr := containerOf(pod, pinnedContainer)
		if containerErr != nil {
			rejected = append(rejected, fmt.Sprintf("%s (%s)", pod.Name, containerErr))

			continue
		}

		if reason := podUnusable(pod, container, declared); reason != "" {
			rejected = append(rejected, fmt.Sprintf("%s (%s)", pod.Name, reason))

			continue
		}

		return Resolved{
			Namespace: pod.Namespace, Pod: pod.Name, Container: container, UID: pod.UID,
		}, nil
	}

	return Resolved{}, fmt.Errorf(
		"%w: %s is labelled %s=%s in %s but none of it can be opened: %s",
		ErrNotFound, name, r.access.NodeLabel, name, r.describeNamespaces(),
		strings.Join(rejected, ", "),
	)
}

func (r *Resolver) resolvePinned(
	ctx context.Context,
	podName string,
	namespace string,
	container string,
) (Resolved, error) {
	if namespace == "" {
		return Resolved{}, fmt.Errorf("%w: a pinned pod needs a namespace too", ErrNotFound)
	}

	// A pinned pod is still subject to the access rules. Without this a lab that hard-binds a pod and
	// then has its namespace list narrowed would keep reaching it, which is exactly the widening the
	// narrowing was meant to prevent.
	if !r.allowsNamespace(namespace) {
		return Resolved{}, fmt.Errorf(
			"%w: %s is not in the configured namespaces %s",
			ErrNotFound, namespace, r.describeNamespaces(),
		)
	}

	pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return Resolved{}, fmt.Errorf("%w: %s/%s: %w", ErrNotFound, namespace, podName, err)
	}

	// A pinned pod fails loudly rather than falling through to another candidate: the lab asked for
	// this pod by name, so substituting a different one would be worse than an error.
	chosen, declared, err := containerOf(*pod, container)
	if err != nil {
		return Resolved{}, fmt.Errorf("%w: %s/%s: %w", ErrNotFound, namespace, podName, err)
	}

	if reason := podUnusable(*pod, chosen, declared); reason != "" {
		return Resolved{}, fmt.Errorf("%w: %s/%s: %s", ErrNotFound, namespace, podName, reason)
	}

	return Resolved{
		Namespace: pod.Namespace, Pod: pod.Name, Container: chosen, UID: pod.UID,
	}, nil
}

// allowsNamespace reports whether the configuration permits a namespace.
func (r *Resolver) allowsNamespace(namespace string) bool {
	return slices.Contains(r.access.Namespaces, "*") ||
		slices.Contains(r.access.Namespaces, namespace)
}

// findByNodeLabel returns the running pods carrying a node label, newest first.
func (r *Resolver) findByNodeLabel(ctx context.Context, node string) ([]corev1.Pod, error) {
	selector := metav1.LabelSelector{
		MatchLabels: map[string]string{r.access.NodeLabel: node},
	}

	if r.access.OwnerLabel != "" && r.access.Owner != "" {
		selector.MatchLabels[r.access.OwnerLabel] = r.access.Owner
	}

	parsed, err := metav1.LabelSelectorAsSelector(&selector)
	if err != nil {
		return nil, fmt.Errorf(
			"building a label selector for %s=%s: %w", r.access.NodeLabel, node, err,
		)
	}

	listOptions := metav1.ListOptions{LabelSelector: parsed.String()}

	var found []corev1.Pod

	for _, namespace := range r.access.Namespaces {
		// "*" is resolved by asking for every namespace: the list call is namespace-scoped, so the
		// wildcard is expanded here rather than passed through, which keeps one code path for both
		// an explicit namespace list and "anywhere".
		namespaces := []string{namespace}
		if namespace == "*" {
			namespaces = []string{metav1.NamespaceAll}
		}

		for _, scope := range namespaces {
			list, err := r.client.CoreV1().Pods(scope).List(ctx, listOptions)
			if err != nil {
				return nil, fmt.Errorf(
					"listing pods labelled %s=%s: %w", r.access.NodeLabel, node, err,
				)
			}

			found = append(found, list.Items...)
		}
	}

	running := make([]corev1.Pod, 0, len(found))

	for _, pod := range found {
		if podIsRunning(pod) {
			running = append(running, pod)
		}
	}

	// Newest first, so a rolling update resolves to the pod that is replacing the old one rather
	// than the pod that is on its way out. The name breaks a tie so the choice is stable when two
	// pods share a creation timestamp to the second.
	sort.SliceStable(running, func(first, second int) bool {
		if !running[first].CreationTimestamp.Equal(&running[second].CreationTimestamp) {
			return running[first].CreationTimestamp.After(running[second].CreationTimestamp.Time)
		}

		return running[first].Name > running[second].Name
	})

	return running, nil
}

// containerOf picks the container to exec into: an explicit pin, then the annotation Clabernetes
// sets to the device container, then the first container.
//
// Falling back to the first container is a last resort rather than a default. On a Clabernetes pod
// the first container is the device, but if the annotation is ever absent the alternative is
// dropping a student into a connectivity sidecar, so the fallback exists only to avoid failing
// outright on a pod shape Clabernetes does not produce.
//
// Every branch is checked against the pod's actual containers rather than trusted. A container name
// that no longer exists -- a stale pin in a lab's configuration, or an annotation left behind by an
// older renderer -- has to be an error, because exec would otherwise report a confusing stream error
// at connect time instead of naming the real problem.
//
// The second result says whether the container was declared or guessed, and it changes how much
// evidence the pod needs before it counts as usable: see podUnusable.
func containerOf(pod corev1.Pod, pinned string) (name string, declared bool, err error) {
	if pinned != "" {
		return pinned, true, containerExists(pod, pinned)
	}

	if annotated, ok := pod.Annotations[config.DefaultContainerAnnotation]; ok && annotated != "" {
		return annotated, true, containerExists(pod, annotated)
	}

	if len(pod.Spec.Containers) == 0 {
		return "", false, fmt.Errorf("has no containers")
	}

	return pod.Spec.Containers[0].Name, false, nil
}

func containerExists(pod corev1.Pod, name string) error {
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return nil
		}
	}

	names := make([]string, 0, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		names = append(names, container.Name)
	}

	return fmt.Errorf("has no container %q (it has %s)", name, strings.Join(names, ", "))
}

// podIsRunning reports whether a pod exists and is running, regardless of container readiness.
func podIsRunning(pod corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning
}

// podUnusable explains why a running pod cannot be exec'd into, or returns "" when it can.
//
// Readiness is judged against how much we actually know about the container.
//
// When the container was declared -- pinned by the lab's configuration, or named by Clabernetes'
// annotation -- only that container's readiness matters. A device pod also runs a clabwire sidecar,
// and that sidecar's readiness flaps during a lab's network setup; requiring it too would hide a
// perfectly usable device behind an unrelated agent's health.
//
// When the container was guessed, there is no evidence that the guess is the device, so the bar is
// the whole pod: if anything in it is still starting, the guess is not yet safe to hand out.
func podUnusable(pod corev1.Pod, container string, declared bool) string {
	if pod.DeletionTimestamp != nil {
		return "is terminating"
	}

	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Sprintf("is %s, not running", pod.Status.Phase)
	}

	ready := map[string]bool{}
	for _, status := range pod.Status.ContainerStatuses {
		ready[status.Name] = status.Ready
	}

	if !declared {
		for _, status := range pod.Status.ContainerStatuses {
			if !status.Ready {
				return fmt.Sprintf(
					"no container is annotated as the device and %s is not ready",
					status.Name,
				)
			}
		}

		return ""
	}

	isReady, reported := ready[container]
	if !reported {
		// No status for the container at all is the state of a pod whose kubelet has not reported yet.
		return fmt.Sprintf("container %s has no status yet", container)
	}

	if !isReady {
		return fmt.Sprintf("container %s is not ready", container)
	}

	return ""
}

func (r *Resolver) describeNamespaces() string {
	if len(r.access.Namespaces) == 1 {
		return "namespace " + r.access.Namespaces[0]
	}

	return "namespaces " + fmt.Sprint(r.access.Namespaces)
}
