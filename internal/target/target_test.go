package target

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/maintainer64/cms-labs-terminal/internal/config"
)

const (
	testNamespace = "srl-ttyd-test"
	testNode      = "srl1"
	testContainer = "node-srl1-primary-0000000"
	testPodName   = "srl1-77f87b7cb4-bk9jz"
)

func access() config.Access {
	return config.Access{
		Namespaces: []string{testNamespace},
		NodeLabel:  config.DefaultNodeLabel,
		OwnerLabel: config.DefaultOwnerLabel,
		Owner:      testNamespace,
	}
}

// devicePod builds a pod shaped like the ones Clabernetes actually renders, because the labels and
// the default-container annotation are the contract this package depends on and both were observed on
// a real cluster rather than assumed.
func devicePod(name, namespace, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				config.DefaultNodeLabel:  node,
				config.DefaultOwnerLabel: namespace,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "clabwire", Image: "ghcr.io/cms-lab-core/clabernetes-clabwire:latest"},
				{Name: testContainer, Image: "ghcr.io/nokia/srlinux:25.3.3"},
			},
			TerminationGracePeriodSeconds: int64Pointer(30),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "clabwire", Ready: true},
				{Name: testContainer, Ready: true},
			},
		},
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

// TestResolvePicksTheDeviceContainerNotTheSidecar is the reason the default-container annotation is
// read at all: an SR Linux pod also contains a clabwire sidecar, and exec-ing into that would give a
// student a connectivity agent instead of a device shell.
func TestResolvePicksTheDeviceContainerNotTheSidecar(t *testing.T) {
	t.Parallel()

	pod := devicePod(testPodName, testNamespace, testNode)
	pod.Annotations = map[string]string{
		config.DefaultContainerAnnotation: testContainer,
	}

	resolver := New(fake.NewSimpleClientset(pod), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Container != testContainer {
		t.Fatalf("container = %q, want the annotated device container %q", resolved.Container, testContainer)
	}

	if resolved.Pod != testPodName || resolved.Namespace != testNamespace {
		t.Fatalf("resolved = %+v, want the device pod", resolved)
	}
}

// TestResolveFallsBackToTheFirstContainer covers a pod without the annotation, which is what a hand
// written manifest or an upstream release produces.
func TestResolveFallsBackToTheFirstContainer(t *testing.T) {
	t.Parallel()

	resolver := New(fake.NewSimpleClientset(devicePod(testPodName, testNamespace, testNode)), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Container != "clabwire" {
		t.Fatalf("container = %q, want the first container when unannotated", resolved.Container)
	}
}

// TestResolveSkipsPendingPods is the difference between a lab that works and one that hands a student
// a terminal to a pod that has not started its device yet.
func TestResolveSkipsPendingPods(t *testing.T) {
	t.Parallel()

	pending := devicePod("srl1-pending", testNamespace, testNode)
	pending.Status.Phase = corev1.PodPending
	pending.Status.ContainerStatuses = nil

	ready := devicePod(testPodName, testNamespace, testNode)
	ready.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(pending, ready), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Pod != testPodName {
		t.Fatalf("pod = %q, want the running pod to win over the pending one", resolved.Pod)
	}
}

// TestResolveSkipsAnUnreadyDevice covers a pod whose container is up but not ready, where exec would
// still connect and then fail somewhere less obvious.
func TestResolveSkipsAnUnreadyDevice(t *testing.T) {
	t.Parallel()

	unready := devicePod("srl1-unready", testNamespace, testNode)
	unready.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "clabwire", Ready: true},
		{Name: testContainer, Ready: false},
	}

	ready := devicePod(testPodName, testNamespace, testNode)
	ready.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(unready, ready), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Pod != testPodName {
		t.Fatalf("pod = %q, want the pod with a ready device container", resolved.Pod)
	}
}

// TestResolveKeepsADeviceWhoseSidecarIsNotReady is the counterpart to the test above, and the reason
// readiness is judged per container rather than per pod. Clabernetes annotates the device container,
// so a clabwire sidecar that is still starting must not make the device unreachable -- which is the
// exact state every lab is in for the first few seconds after `topo up`.
func TestResolveKeepsADeviceWhoseSidecarIsNotReady(t *testing.T) {
	t.Parallel()

	starting := devicePod(testPodName, testNamespace, testNode)
	starting.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}
	starting.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "clabwire", Ready: false},
		{Name: testContainer, Ready: true},
	}

	resolver := New(fake.NewSimpleClientset(starting), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Container != testContainer {
		t.Fatalf("container = %q, want the annotated device", resolved.Container)
	}
}

// TestResolveSkipsTerminatingPods covers a lab teardown: exec into a pod that is being deleted hangs
// or returns an error the student cannot act on.
func TestResolveSkipsTerminatingPods(t *testing.T) {
	t.Parallel()

	dying := devicePod("srl1-dying", testNamespace, testNode)
	dying.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	dying.Finalizers = []string{"clabernetes.example/cleanup"}

	ready := devicePod(testPodName, testNamespace, testNode)
	ready.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(dying, ready), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Pod != testPodName {
		t.Fatalf("pod = %q, want the terminating pod skipped", resolved.Pod)
	}
}

// TestResolveHonoursTheOwnerLabel is what stops a terminal from reaching the pod of a *different*
// topology that happens to share a node name, which is the realistic multi-topology case.
func TestResolveHonoursTheOwnerLabel(t *testing.T) {
	t.Parallel()

	other := devicePod("srl1-other", "other-lab", testNode)
	other.Labels[config.DefaultOwnerLabel] = "other-lab"
	other.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	mine := devicePod(testPodName, testNamespace, testNode)
	mine.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(other, mine), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Namespace != testNamespace {
		t.Fatalf("namespace = %q, want the pod owned by this topology", resolved.Namespace)
	}
}

// TestResolveRefusesANamespaceOutsideTheAccessList is the RBAC-shaped test done in policy rather than
// in the cluster: even with API permission, an unlisted namespace is not reachable.
func TestResolveRefusesANamespaceOutsideTheAccessList(t *testing.T) {
	t.Parallel()

	policy := access()
	policy.Namespaces = []string{"allowed-lab"}
	policy.Owner = ""

	outside := devicePod(testPodName, "kube-system", testNode)
	outside.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(outside), policy)

	_, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

// TestResolveRefusesAnExplicitPodOutsideTheAccessList covers a link crafted by hand to name a pod
// directly, bypassing the node label entirely.
func TestResolveRefusesAnExplicitPodOutsideTheAccessList(t *testing.T) {
	t.Parallel()

	policy := access()
	policy.Namespaces = []string{"allowed-lab"}
	policy.Owner = ""

	elsewhere := devicePod("victim-abc", "kube-system", "other")

	resolver := New(fake.NewSimpleClientset(elsewhere), policy)

	_, err := resolver.Resolve(context.Background(), "victim-abc", "victim-abc", "", "")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

// TestResolveRejectsAPinnedPodFromAnotherNamespace covers a configuration that hard-binds a pod and
// then loses access to it, which must fail loudly rather than silently widening.
func TestResolveRejectsAPinnedPodFromAnotherNamespace(t *testing.T) {
	t.Parallel()

	pinned := devicePod(testPodName, "other-lab", testNode)
	pinned.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(pinned), access())

	_, err := resolver.Resolve(
		context.Background(), testNode, testPodName, "other-lab", "",
	)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

// TestResolveRejectsAnUnknownContainer keeps a stale container name in a lab's configuration from
// resolving to some other container that happens to still exist.
func TestResolveRejectsAnUnknownContainer(t *testing.T) {
	t.Parallel()

	pod := devicePod(testPodName, testNamespace, testNode)

	resolver := New(fake.NewSimpleClientset(pod), access())

	_, err := resolver.Resolve(
		context.Background(), testNode, "", "", "no-such-container",
	)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

// TestResolvePrefersANewerPod covers a rolling update: two pods can carry the same node label, and
// sending a student to the one being deleted is worse than sending them to the new one.
func TestResolvePrefersANewerPod(t *testing.T) {
	t.Parallel()

	old := devicePod("srl1-old", testNamespace, testNode)
	old.CreationTimestamp = metav1.Time{Time: metav1.Now().Add(-2 * time.Hour)}
	old.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	fresh := devicePod("srl1-new", testNamespace, testNode)
	fresh.CreationTimestamp = metav1.Time{Time: metav1.Now().Time}
	fresh.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(old, fresh), access())

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Pod != "srl1-new" {
		t.Fatalf("pod = %q, want the newest pod", resolved.Pod)
	}
}

// TestResolveReportsAClearErrorForAMissingNode covers the case a student actually hits: a stale
// bookmark after the lab was torn down.
func TestResolveReportsAClearErrorForAMissingNode(t *testing.T) {
	t.Parallel()

	resolver := New(fake.NewSimpleClientset(), access())

	_, err := resolver.Resolve(context.Background(), "gone", "", "", "")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

// TestResolveWorksAcrossNamespaces covers the wildcard a multi-namespace lab needs, while the owner
// label still keeps the topology separate.
func TestResolveWorksAcrossNamespaces(t *testing.T) {
	t.Parallel()

	policy := access()
	policy.Namespaces = []string{"*"}
	policy.Owner = testNamespace

	pod := devicePod(testPodName, testNamespace, testNode)
	pod.Annotations = map[string]string{config.DefaultContainerAnnotation: testContainer}

	resolver := New(fake.NewSimpleClientset(pod), policy)

	resolved, err := resolver.Resolve(context.Background(), testNode, "", "", "")
	if err != nil {
		t.Fatalf("Resolve: %s", err)
	}

	if resolved.Namespace != testNamespace {
		t.Fatalf("namespace = %q, want the discovered namespace", resolved.Namespace)
	}
}
