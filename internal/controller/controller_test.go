package controller

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	"github.com/cms-lab-core/cms-labs-terminal/internal/config"
)

func TestReconcileCreatesNamespaceLocalRuntimeFromMinimalConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset(
		managedNamespace("lab-a"),
		terminalSource("lab-a", `
server:
  address: 127.0.0.1
  authHeader: Unsafe-Header
  allowUnauthenticated: true
broker:
  lease: 23h
access:
  namespaces: [foreign]
targets:
  - name: r1
    namespace: foreign
    port: 80
    command: [/bin/ash, -l]
  - name: srl1
    command: [sr_cli]
`),
	)
	operator := testController(t, client)
	if err := operator.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}

	deployment, err := client.AppsV1().Deployments("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Template.Spec.ServiceAccountName != RuntimeName ||
		deployment.Spec.Template.Spec.Containers[0].Image != "example.test/terminal:1" {
		t.Fatalf("deployment = %#v", deployment.Spec.Template.Spec)
	}
	for _, name := range []string{"r1-terminal", "srl1-terminal"} {
		service, getErr := client.CoreV1().Services("lab-a").Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			t.Fatal(getErr)
		}
		if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Name != "ttyd" || service.Spec.Ports[0].Port != 7681 {
			t.Fatalf("service %s = %#v", name, service.Spec.Ports)
		}
	}
	runtimeConfig, err := client.CoreV1().ConfigMaps("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := config.Decode([]byte(runtimeConfig.Data["config.yaml"]))
	if err != nil {
		t.Fatal(err)
	}
	if err = parsed.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(parsed.Access.Namespaces) != 1 || parsed.Access.Namespaces[0] != "lab-a" ||
		parsed.Server.Address != config.DefaultAddress || parsed.Server.AuthHeader != config.DefaultAuthHeader ||
		parsed.Server.AllowUnauthenticated || parsed.Broker.Lease.Duration != 30*time.Minute ||
		parsed.Targets[0].Namespace != "lab-a" || parsed.Targets[0].Port != 7681 || parsed.Targets[1].Port != 7682 {
		t.Fatalf("runtime config = %#v", parsed)
	}
	if _, err = client.NetworkingV1().NetworkPolicies("lab-a").Get(ctx, RuntimeName, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	writes := mutationCount(client.Actions())
	if err = operator.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if current := mutationCount(client.Actions()); current != writes {
		t.Fatalf("unchanged reconcile performed %d writes", current-writes)
	}
}

func TestReconcilePrunesRemovedTargetService(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := terminalSource("lab-a", "targets:\n  - name: r1\n  - name: r2\n")
	client := fake.NewClientset(managedNamespace("lab-a"), source)
	operator := testController(t, client)
	if err := operator.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := client.CoreV1().ConfigMaps("lab-a").Get(ctx, DefaultSourceName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source.Data["config.yaml"] = "targets:\n  - name: r1\n"
	if _, err = client.CoreV1().ConfigMaps("lab-a").Update(ctx, source, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = operator.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CoreV1().Services("lab-a").Get(ctx, "r2-terminal", metav1.GetOptions{}); err == nil {
		t.Fatal("removed target Service still exists")
	}
}

func TestReconcileRejectsUnmanagedNamespace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "foreign"}},
		terminalSource("foreign", "targets:\n  - name: r1\n"),
	)
	operator := testController(t, client)
	if err := operator.ReconcileAll(ctx); err == nil {
		t.Fatal("unmanaged namespace was accepted")
	}
	if _, err := client.AppsV1().Deployments("foreign").Get(ctx, RuntimeName, metav1.GetOptions{}); err == nil {
		t.Fatal("runtime was created in an unmanaged namespace")
	}
}

func testController(t *testing.T, client *fake.Clientset) *Controller {
	t.Helper()
	operator, err := New(client, Options{
		Image: "example.test/terminal:1", ProxyNamespace: "cms-labs-system",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return operator
}

func managedNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: name, Labels: map[string]string{DefaultManagedNamespaceLabel: DefaultManagedNamespaceValue},
	}}
}

func terminalSource(namespace, data string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: DefaultSourceName, Namespace: namespace, UID: types.UID("source-uid"),
		Labels: map[string]string{DefaultSourceLabel: "true"},
	}, Data: map[string]string{"config.yaml": data}}
}

func mutationCount(actions []kubetesting.Action) int {
	count := 0
	for _, action := range actions {
		switch action.GetVerb() {
		case "create", "update", "patch", "delete":
			count++
		}
	}
	return count
}
