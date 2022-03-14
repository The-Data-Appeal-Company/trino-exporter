package kubernetes

import (
	"context"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"strings"
	"testing"
)

type k8sClient struct {
	kubernetes.Interface
}

type mockCoreV1 struct {
	corev1.CoreV1Interface
}

type mockNamespace struct {
	corev1.NamespaceInterface
}

type mockServiceDefault struct {
	corev1.ServiceInterface
}

type mockServiceNs1 struct {
	corev1.ServiceInterface
}

type mockServiceNs2 struct {
	corev1.ServiceInterface
}

func (c k8sClient) CoreV1() corev1.CoreV1Interface {
	return mockCoreV1{}
}

func (mc mockCoreV1) Namespaces() corev1.NamespaceInterface {
	return mockNamespace{}
}

func (mc mockCoreV1) Services(namespace string) corev1.ServiceInterface {
	switch namespace {
	case "ns-1":
		return mockServiceNs1{}
	case "ns-2":
		return mockServiceNs2{}
	}

	return mockServiceDefault{}
}

func (ms mockServiceDefault) List(ctx context.Context, opts metav1.ListOptions) (*v1.ServiceList, error) {
	return &v1.ServiceList{
		TypeMeta: metav1.TypeMeta{},
		ListMeta: metav1.ListMeta{},
		Items:    []v1.Service{},
	}, nil
}

func (ms mockServiceNs1) List(ctx context.Context, opts metav1.ListOptions) (*v1.ServiceList, error) {

	if strings.Contains(opts.LabelSelector, "trino.distribution=trino-exportersql") {

		return &v1.ServiceList{
			TypeMeta: metav1.TypeMeta{},
			ListMeta: metav1.ListMeta{},
			Items: []v1.Service{
				{ObjectMeta: metav1.ObjectMeta{
					Name:      "trino-exportersql-1",
					Namespace: "ns-1",
				},
					Spec: v1.ServiceSpec{
						Ports: []v1.ServicePort{
							{
								Name:     svcPortName,
								Protocol: "TCP",
								Port:     8888,
							},
						},
					}},
				{ObjectMeta: metav1.ObjectMeta{
					Name:      "trino-exportersql-12",
					Namespace: "ns-1",
				},
					Spec: v1.ServiceSpec{
						Ports: []v1.ServicePort{
							{
								Name:     svcPortName,
								Protocol: "TCP",
								Port:     8888,
							},
						},
					},
				},
			},
		}, nil
	}

	return &v1.ServiceList{
		TypeMeta: metav1.TypeMeta{},
		ListMeta: metav1.ListMeta{},
		Items: []v1.Service{
			{ObjectMeta: metav1.ObjectMeta{
				Name:      "trino-exporterdb-1",
				Namespace: "ns-1",
			}, Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{
					{
						Name:     svcPortName,
						Protocol: "TCP",
						Port:     8888,
					},
				},
			}},
		},
	}, nil

}

func (ms mockServiceNs2) List(ctx context.Context, opts metav1.ListOptions) (*v1.ServiceList, error) {
	if strings.Contains(opts.LabelSelector, "trino.distribution=trino-exportersql") {
		return &v1.ServiceList{
			TypeMeta: metav1.TypeMeta{},
			ListMeta: metav1.ListMeta{},
			Items: []v1.Service{
				{ObjectMeta: metav1.ObjectMeta{
					Name:      "trino-exportersql-2",
					Namespace: "ns-2",
				}, Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:     svcPortName,
							Protocol: "TCP",
							Port:     8888,
						},
					},
				}},
			},
		}, nil
	}
	return &v1.ServiceList{
		TypeMeta: metav1.TypeMeta{},
		ListMeta: metav1.ListMeta{},
		Items:    []v1.Service{},
	}, nil
}

func (mn mockNamespace) List(ctx context.Context, opts metav1.ListOptions) (*v1.NamespaceList, error) {
	return &v1.NamespaceList{

		Items: []v1.Namespace{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "default",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns-1",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns-2",
				},
			},
		},
	}, nil
}

func TestClusterProviderKubernetes(t *testing.T) {

	clientset := fake.NewSimpleClientset()

	client := k8sClient{clientset}

	provider := NewClusterProvider(client, "cluster.local", "")

	clusters, err := provider.Provide()
	require.NoError(t, err)

	require.Len(t, clusters, 1)

}

func TestClusterProviderKubernetesWithSvcLabelSelector(t *testing.T) {

	clientset := fake.NewSimpleClientset()

	client := k8sClient{clientset}

	provider := NewClusterProvider(client, "cluster.local", "trino.distribution=trino-exportersql")

	clusters, err := provider.Provide()
	require.NoError(t, err)

	require.Len(t, clusters, 3)

}
