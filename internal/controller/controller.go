// Package controller reconciles minimal per-lab terminal declarations into namespace-local
// terminal brokers. The controller is installed once per cluster by the Helm chart.
package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	"github.com/cms-lab-core/cms-labs-terminal/internal/config"
)

const (
	DefaultSourceName            = "cms-labs-terminal-config"
	DefaultSourceLabel           = "cms-labs.io/terminal-config"
	DefaultManagedNamespaceLabel = "app.kubernetes.io/managed-by"
	DefaultManagedNamespaceValue = "clabgate"
	DefaultProxyNamespace        = "cms-labs-system"
	RuntimeName                  = "cms-labs-terminal"
	firstTargetPort              = 7681
	managedByValue               = "cms-labs-terminal-controller"
)

type Options struct {
	Image                 string
	ImagePullPolicy       corev1.PullPolicy
	ReconcileInterval     time.Duration
	SourceName            string
	SourceLabel           string
	ManagedNamespaceLabel string
	ManagedNamespaceValue string
	ProxyNamespace        string
}

type Controller struct {
	client  kubernetes.Interface
	options Options
	logger  *slog.Logger
}

func New(client kubernetes.Interface, options Options, logger *slog.Logger) (*Controller, error) {
	if client == nil {
		return nil, errors.New("kubernetes client is required")
	}
	if options.Image == "" {
		return nil, errors.New("runtime image is required")
	}
	if options.ImagePullPolicy == "" {
		options.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if options.ReconcileInterval <= 0 {
		options.ReconcileInterval = 15 * time.Second
	}
	if options.SourceName == "" {
		options.SourceName = DefaultSourceName
	}
	if options.SourceLabel == "" {
		options.SourceLabel = DefaultSourceLabel
	}
	if options.ManagedNamespaceLabel == "" {
		options.ManagedNamespaceLabel = DefaultManagedNamespaceLabel
	}
	if options.ManagedNamespaceValue == "" {
		options.ManagedNamespaceValue = DefaultManagedNamespaceValue
	}
	if options.ProxyNamespace == "" {
		options.ProxyNamespace = DefaultProxyNamespace
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Controller{client: client, options: options, logger: logger}, nil
}

func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.options.ReconcileInterval)
	defer ticker.Stop()
	for {
		if err := c.ReconcileAll(ctx); err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error("terminal controller reconcile failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *Controller) ReconcileAll(ctx context.Context) error {
	selector := labels.Set{c.options.SourceLabel: "true"}.String()
	sources, err := c.client.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("listing terminal declarations: %w", err)
	}
	var result error
	for index := range sources.Items {
		source := &sources.Items[index]
		if source.Name != c.options.SourceName {
			continue
		}
		if err = c.Reconcile(ctx, source); err != nil {
			result = errors.Join(result, fmt.Errorf("%s/%s: %w", source.Namespace, source.Name, err))
		}
	}
	return result
}

func (c *Controller) Reconcile(ctx context.Context, source *corev1.ConfigMap) error {
	if source == nil || source.Namespace == "" || source.Name != c.options.SourceName {
		return errors.New("invalid terminal declaration identity")
	}
	namespace, err := c.client.CoreV1().Namespaces().Get(ctx, source.Namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading namespace: %w", err)
	}
	if namespace.Labels[c.options.ManagedNamespaceLabel] != c.options.ManagedNamespaceValue {
		return fmt.Errorf("namespace is not managed by %s=%s", c.options.ManagedNamespaceLabel, c.options.ManagedNamespaceValue)
	}
	raw := source.Data["config.yaml"]
	if raw == "" {
		return errors.New("config.yaml is required")
	}
	configuration, err := config.Decode([]byte(raw))
	if err != nil {
		return fmt.Errorf("decoding config.yaml: %w", err)
	}
	// Networking, authentication, broker lifetime and namespace boundaries are
	// operator policy. A lab declaration controls only shell behavior and targets.
	configuration.Server = config.Server{}
	configuration.Broker = config.Broker{}
	configuration.Access = config.Access{
		Namespaces: []string{source.Namespace},
		NodeLabel:  config.DefaultNodeLabel,
		OwnerLabel: config.DefaultOwnerLabel,
		Owner:      source.Namespace,
	}
	for index := range configuration.Targets {
		configuration.Targets[index].Port = firstTargetPort + index
		configuration.Targets[index].Namespace = source.Namespace
	}
	if err = configuration.Validate(); err != nil {
		return err
	}

	owner := metav1.OwnerReference{
		APIVersion: "v1", Kind: "ConfigMap", Name: source.Name, UID: source.UID,
		Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
	}
	if err = c.applyRuntime(ctx, source.Namespace, source.Name, owner, configuration); err != nil {
		return err
	}
	c.logger.Debug("terminal runtime reconciled", "namespace", source.Namespace, "targets", len(configuration.Targets))
	return nil
}

func runtimeLabels(sourceName string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       RuntimeName,
		"app.kubernetes.io/managed-by": managedByValue,
		"cms-labs.io/source-config":    sourceName,
	}
}

func (c *Controller) applyRuntime(
	ctx context.Context,
	namespace string,
	sourceName string,
	owner metav1.OwnerReference,
	configuration config.Config,
) error {
	labelsMap := runtimeLabels(sourceName)
	metadata := metav1.ObjectMeta{
		Name: RuntimeName, Namespace: namespace, Labels: labelsMap, OwnerReferences: []metav1.OwnerReference{owner},
	}
	raw, err := yaml.Marshal(configuration)
	if err != nil {
		return fmt.Errorf("encoding runtime configuration: %w", err)
	}
	if err = c.applyConfigMap(ctx, &corev1.ConfigMap{ObjectMeta: metadata, Data: map[string]string{"config.yaml": string(raw)}}); err != nil {
		return err
	}
	if err = c.applyServiceAccount(ctx, &corev1.ServiceAccount{ObjectMeta: metadata, AutomountServiceAccountToken: ptr.To(true)}); err != nil {
		return err
	}
	role := &rbacv1.Role{ObjectMeta: metadata, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec", "pods/attach"}, Verbs: []string{"get", "create"}},
	}}
	if err = c.applyRole(ctx, role); err != nil {
		return err
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metadata,
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: RuntimeName, Namespace: namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: RuntimeName},
	}
	if err = c.applyRoleBinding(ctx, binding); err != nil {
		return err
	}
	if err = c.applyDeployment(ctx, terminalDeployment(metadata, c.options, configuration)); err != nil {
		return err
	}
	desiredServices := make(map[string]struct{}, len(configuration.Targets))
	for index, terminalTarget := range configuration.Targets {
		name := terminalTarget.Name + "-terminal"
		if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
			return fmt.Errorf("terminal Service name %q is invalid: %v", name, problems)
		}
		desiredServices[name] = struct{}{}
		serviceMetadata := metadata
		serviceMetadata.Name = name
		serviceMetadata.Annotations = map[string]string{"cms-labs.io/terminal-target": terminalTarget.Name}
		service := &corev1.Service{
			ObjectMeta: serviceMetadata,
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app.kubernetes.io/name": RuntimeName},
				Ports: []corev1.ServicePort{{
					Name: "ttyd", Port: 7681, TargetPort: intstr.FromString(fmt.Sprintf("terminal-%d", index)),
				}},
			},
		}
		if err = c.applyService(ctx, service); err != nil {
			return err
		}
	}
	if err = c.pruneServices(ctx, namespace, sourceName, desiredServices); err != nil {
		return err
	}
	if err = c.applyNetworkPolicy(ctx, terminalNetworkPolicy(metadata, c.options.ProxyNamespace, configuration)); err != nil {
		return err
	}
	return nil
}

func terminalDeployment(metadata metav1.ObjectMeta, options Options, configuration config.Config) *appsv1.Deployment {
	ports := make([]corev1.ContainerPort, 0, len(configuration.Targets))
	for index, target := range configuration.Targets {
		ports = append(ports, corev1.ContainerPort{
			Name:          fmt.Sprintf("terminal-%d", index),
			ContainerPort: int32(target.Port), //nolint:gosec // config validation bounds ports to 1..65535.
		})
	}
	probePort := int32(configuration.Targets[0].Port) //nolint:gosec // config validation bounds ports to 1..65535.
	selector := map[string]string{"app.kubernetes.io/name": RuntimeName}
	return &appsv1.Deployment{
		ObjectMeta: metadata,
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)), Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec: corev1.PodSpec{
					ServiceAccountName: RuntimeName, TerminationGracePeriodSeconds: ptr.To(int64(15)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(65532)), RunAsGroup: ptr.To(int64(65532)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "terminal", Image: options.Image, ImagePullPolicy: options.ImagePullPolicy,
						Args: []string{"serve"}, Ports: ports,
						Env:          []corev1.EnvVar{{Name: "CMS_LABS_TERMINAL_CONFIG", Value: "/etc/cms-labs-terminal/config.yaml"}},
						VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/etc/cms-labs-terminal", ReadOnly: true}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
							Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(probePort)}}, InitialDelaySeconds: 1, PeriodSeconds: 5},
						LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(probePort)}}, InitialDelaySeconds: 5, PeriodSeconds: 20},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
						},
					}},
					Volumes: []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: RuntimeName}}}}},
				},
			},
		},
	}
}

func terminalNetworkPolicy(metadata metav1.ObjectMeta, proxyNamespace string, configuration config.Config) *networkingv1.NetworkPolicy {
	ports := make([]networkingv1.NetworkPolicyPort, 0, len(configuration.Targets))
	for _, target := range configuration.Targets {
		protocol := corev1.ProtocolTCP
		port := intstr.FromInt(target.Port)
		ports = append(ports, networkingv1.NetworkPolicyPort{Protocol: &protocol, Port: &port})
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metadata,
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": RuntimeName}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From:  []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": proxyNamespace}}}},
				Ports: ports,
			}},
		},
	}
}

func (c *Controller) applyConfigMap(ctx context.Context, desired *corev1.ConfigMap) error {
	api := c.client.CoreV1().ConfigMaps(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyServiceAccount(ctx context.Context, desired *corev1.ServiceAccount) error {
	api := c.client.CoreV1().ServiceAccounts(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		desired.Secrets = existing.Secrets
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyRole(ctx context.Context, desired *rbacv1.Role) error {
	api := c.client.RbacV1().Roles(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyRoleBinding(ctx context.Context, desired *rbacv1.RoleBinding) error {
	api := c.client.RbacV1().RoleBindings(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyDeployment(ctx context.Context, desired *appsv1.Deployment) error {
	api := c.client.AppsV1().Deployments(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyService(ctx context.Context, desired *corev1.Service) error {
	api := c.client.CoreV1().Services(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		desired.Spec.ClusterIP = existing.Spec.ClusterIP
		desired.Spec.ClusterIPs = existing.Spec.ClusterIPs
		desired.Spec.IPFamilies = existing.Spec.IPFamilies
		desired.Spec.IPFamilyPolicy = existing.Spec.IPFamilyPolicy
		desired.Spec.HealthCheckNodePort = existing.Spec.HealthCheckNodePort
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) applyNetworkPolicy(ctx context.Context, desired *networkingv1.NetworkPolicy) error {
	api := c.client.NetworkingV1().NetworkPolicies(desired.Namespace)
	existing, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	} else if err == nil {
		if apiequality.Semantic.DeepDerivative(desired, existing) {
			return nil
		}
		desired.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	}
	return err
}

func (c *Controller) pruneServices(ctx context.Context, namespace, sourceName string, desired map[string]struct{}) error {
	selector := labels.Set{
		"app.kubernetes.io/managed-by": managedByValue,
		"cms-labs.io/source-config":    sourceName,
	}.String()
	services, err := c.client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	for index := range services.Items {
		name := services.Items[index].Name
		if _, keep := desired[name]; keep || !slices.ContainsFunc(services.Items[index].OwnerReferences, func(reference metav1.OwnerReference) bool { return reference.Name == sourceName }) {
			continue
		}
		if err = c.client.CoreV1().Services(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
