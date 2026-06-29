package mesh

import (
	"context"
	"fmt"

	meshv1alpha1 "github.com/stolostron/multicluster-mesh-addon/pkg/apis/mesh/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"
	msav1beta1 "open-cluster-management.io/managed-serviceaccount/apis/authentication/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	multiClusterSecretLabel          = "istio/multiCluster"
	msaRootWord                      = "istio-reader"
	remoteSecretRootWord             = "istio-remote-secret"
	remoteSecretLabel                = "istio/multiCluster"
	remoteSecretAnnotation           = "networking.istio.io/cluster"
	remoteSecretManifestWorkRootWord = "multicluster-mesh-remote-secrets"
)

// ensureManagedServiceAccountCreated creates ManagedServiceAccount resources for a cluster.
// It checks and uses the mesh.Spec.Security.Discovery.TokenValidity value.
func (r *Reconciler) ensureManagedServiceAccountCreated(ctx context.Context, mesh *meshv1alpha1.MultiClusterMesh, cluster *clusterv1.ManagedCluster) error {
	msaName := fmt.Sprintf("%s-%s-%s", mesh.Namespace, msaRootWord, mesh.Name)
	existing := &msav1beta1.ManagedServiceAccount{}
	if err := r.Get(ctx, types.NamespacedName{Name: msaName, Namespace: cluster.Name}, existing); err == nil {
		if existing.Labels[MeshNameLabel] == mesh.Name || existing.Labels[MeshNamespaceLabel] == mesh.Namespace {
			return r.ensureManagedServiceAccountUpdated(ctx, mesh, existing)
		}
		// else: existing msa is not owned by the mesh, skipping update and continue creation
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to get ManagedServiceAccount %s/%s: %w", cluster.Name, msaName, err)
	}

	msa := &msav1beta1.ManagedServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      msaName,
			Namespace: cluster.Name,
			Labels:    meshOwnedLabels(mesh, cluster.Name),
		},
		Spec: msav1beta1.ManagedServiceAccountSpec{
			Rotation: msav1beta1.ManagedServiceAccountRotation{
				Enabled:  true,
				Validity: *mesh.Spec.Security.Discovery.TokenValidity,
			},
		},
	}

	if err := r.Create(ctx, msa); err != nil {
		return fmt.Errorf("failed to create a ManagedServiceAccount %s/%s: %w", cluster.Name, msaName, err)
	}

	klog.Infof("Successfully created a ManagedServiceAccount %s/%s", cluster.Name, msaName)
	return nil
}

// cleanupManagedServiceAccounts deletes ManagedServiceAccount and istio-remote secret,
// when the cluster(s) are removed from the given mesh's ClusterSet.
func (r *Reconciler) cleanupManagedServiceAccounts(ctx context.Context, mesh *meshv1alpha1.MultiClusterMesh, clusters []clusterv1.ManagedCluster) error {
	clusterNames := clusterNameSet(clusters)

	msaList := &msav1beta1.ManagedServiceAccountList{}
	if err := r.List(ctx, msaList,
		client.MatchingLabels{MeshNameLabel: mesh.Name, MeshNamespaceLabel: mesh.Namespace}); err != nil {
		return fmt.Errorf("failed to list ManagedServiceAccounts: %w", err)
	}

	secretList := &corev1.SecretList{}
	if err := r.List(ctx, secretList, client.InNamespace(mesh.Namespace), client.MatchingLabels{
		multiClusterSecretLabel: "true", MeshNameLabel: mesh.Name, MeshNamespaceLabel: mesh.Namespace,
	}); err != nil {
		return fmt.Errorf("failed to list istio-remote secrets managed by mesh %s: %w", mesh.Name, err)
	}

	for _, msa := range msaList.Items {
		clusterName := msa.Labels[ClusterNameLabel]
		if clusterNames[clusterName] {
			continue
		}

		klog.Infof("Deleting ManagedServiceAccount %s/%s (cluster %s no longer in ClusterSet %s)", msa.Namespace, msa.Name, clusterName, mesh.Spec.ClusterSet)
		if err := client.IgnoreNotFound(r.Delete(ctx, &msa)); err != nil {
			return fmt.Errorf("failed to delete ManagedServiceAccount %s/%s: %w", msa.Namespace, msa.Name, err)
		}
	}

	for _, sec := range secretList.Items {
		clusterName := sec.Labels[ClusterNameLabel]
		if clusterNames[clusterName] {
			continue
		}

		klog.Infof("Deleting istio remote secret %s/%s (cluster %s no longer in ClusterSet %s)", sec.Namespace, sec.Name, clusterName, mesh.Spec.ClusterSet)
		if err := client.IgnoreNotFound(r.Delete(ctx, &sec)); err != nil {
			return fmt.Errorf("failed to delete istio remote secret %s/%s: %w", sec.Namespace, sec.Name, err)
		}
	}

	return nil
}

// deleteAllManagedServiceAccounts deletes all ManagedServiceAccount resources managed by a mesh
func (r *Reconciler) deleteAllManagedServiceAccounts(ctx context.Context, mesh *meshv1alpha1.MultiClusterMesh) error {
	msaList := &msav1beta1.ManagedServiceAccountList{}
	if err := r.List(ctx, msaList, client.MatchingLabels{MeshNameLabel: mesh.Name, MeshNamespaceLabel: mesh.Namespace}); err != nil {
		return fmt.Errorf("failed to list ManagedServiceAccount resources managed by mesh %s: %w", mesh.Name, err)
	}

	secretList := &corev1.SecretList{}
	if err := r.List(ctx, secretList, client.InNamespace(mesh.Namespace), client.MatchingLabels{
		multiClusterSecretLabel: "true", MeshNameLabel: mesh.Name, MeshNamespaceLabel: mesh.Namespace,
	}); err != nil {
		return fmt.Errorf("failed to list istio-remote secrets managed by mesh %s: %w", mesh.Name, err)
	}

	for _, msa := range msaList.Items {
		klog.Infof("Deleting ManagedServiceAccount %s/%s", msa.Namespace, msa.Name)
		if err := client.IgnoreNotFound(r.Delete(ctx, &msa)); err != nil {
			return fmt.Errorf("failed to delete ManagedServiceAccount %s/%s: %w", msa.Namespace, msa.Name, err)
		}
	}

	for _, sec := range secretList.Items {
		klog.Infof("Deleting an istio remote secret %s", sec.Name)
		if err := client.IgnoreNotFound(r.Delete(ctx, &sec)); err != nil {
			return fmt.Errorf("failed to delete an istio remote secret %s: %w", sec.Name, err)
		}
	}

	return nil
}

// ensureManagedServiceAccountUpdated updates an existing ManagedServiceAccount with the mesh's spec.Security.Discovery.TokenValidity value
func (r *Reconciler) ensureManagedServiceAccountUpdated(ctx context.Context, mesh *meshv1alpha1.MultiClusterMesh, existing *msav1beta1.ManagedServiceAccount) error {
	existing.Spec.Rotation.Validity = *mesh.Spec.Security.Discovery.TokenValidity
	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("failed to update a ManagedServiceAccount %s/%s: %w", existing.Namespace, existing.Name, err)
	} else {
		klog.V(4).Infof("Successfully updated a ManagedServiceAccount %s/%s", existing.Namespace, existing.Name)
	}
	return nil
}

// ensureRemoteSecretDistributed creates a ManifestWork to distribute the Istio remote access secrets.
// The ManagedServiceAccount controller generates an access secret with the name of the ManagedServiceAccount resource.
// That ManagedServiceAccount secret is not distributed directly.
// We build an Istio remote access secret in the method buildMeshRemoteSecret. And then it is added in a ManifestWork's workload.
func (r *Reconciler) ensureRemoteSecretDistributed(ctx context.Context, mesh *meshv1alpha1.MultiClusterMesh, cluster *clusterv1.ManagedCluster) (*workv1.ManifestWork, error) {
	msaSecretName := fmt.Sprintf("%s-%s-%s", mesh.Namespace, msaRootWord, mesh.Name)
	msaSecret := &corev1.Secret{}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, types.NamespacedName{Name: msaSecretName, Namespace: cluster.Name}, msaSecret); err != nil {
			klog.V(4).Infof("ManagedServiceAccount secret %s/%s not found yet, waiting for its controller to create it", cluster.Name, msaSecretName)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get ManagedServiceAccount secret %s/%s: %w", cluster.Name, msaSecretName, err)
	}

	return r.workApplier.Apply(ctx, r.buildRemoteSecretManifestWork(mesh, cluster, r.buildMeshRemoteSecret(mesh, cluster, msaSecret)))
}

// buildRemoteSecretManifestWork builds a ManifestWork using the Istio remote secret above.
func (r *Reconciler) buildRemoteSecretManifestWork(mesh *meshv1alpha1.MultiClusterMesh, cluster *clusterv1.ManagedCluster, remoteSecret *corev1.Secret) *workv1.ManifestWork {
	remoteSecretManifestWorkName := fmt.Sprintf("%s-%s-%s", mesh.Namespace, remoteSecretManifestWorkRootWord, mesh.Name)
	return &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      remoteSecretManifestWorkName,
			Namespace: cluster.Name,
			Labels:    meshOwnedLabels(mesh, cluster.Name),
		},
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{
				Manifests: []workv1.Manifest{{
					RawExtension: runtime.RawExtension{Object: remoteSecret},
				}},
			},
			ManifestConfigs: []workv1.ManifestConfigOption{{
				UpdateStrategy: &workv1.UpdateStrategy{
					Type: workv1.UpdateStrategyTypeServerSideApply,
				},
			}},
		},
	}
}

// buildMeshRemoteSecret builds an Istio remote API server access secret using a secret data
// which is generated by the ManagedServiceAccount controller.
// It builds the secret using additional label and annotation from the upstream Istio remote_secret example.
// reference: https://github.com/istio/istio/blob/master/istioctl/pkg/multicluster/remote_secret.go
func (r *Reconciler) buildMeshRemoteSecret(mesh *meshv1alpha1.MultiClusterMesh, cluster *clusterv1.ManagedCluster, msaSecret *corev1.Secret) *corev1.Secret {
	istioRemoteSecretName := fmt.Sprintf("%s-%s-%s", mesh.Namespace, remoteSecretRootWord, mesh.Name)
	istioRemoteSecretLabels := meshOwnedLabels(mesh, cluster.Name)
	istioRemoteSecretLabels[multiClusterSecretLabel] = "true"

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      istioRemoteSecretName,
			Namespace: mesh.GetControlPlaneNamespace(),
			Labels:    istioRemoteSecretLabels,
			Annotations: map[string]string{
				remoteSecretAnnotation: cluster.Name,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: msaSecret.Data,
	}
}
