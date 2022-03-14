package kubernetes

import (
	"context"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	v12 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/url"
	"time"
	"trino-exporter/trino"
)

const (
	svcPortName = "http"
)

type ClusterProvider struct {
	k8sClient        k8s.Interface
	cache            *cache.Cache
	clusterDomain    string
	svcLabelSelector string
}

func NewInClusterProvider(clusterDomain string, svcLabelSelector string) (*ClusterProvider, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	k8sClient, err := k8s.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return NewClusterProvider(k8sClient, clusterDomain, svcLabelSelector), nil
}

func NewClusterProvider(k8sClient k8s.Interface, clusterDomain string, svcLabelSelector string) *ClusterProvider {
	return &ClusterProvider{
		k8sClient:        k8sClient,
		clusterDomain:    clusterDomain,
		svcLabelSelector: svcLabelSelector,
		cache:            cache.New(10*time.Minute, 24*time.Hour),
	}
}

const cacheKey = "k8s-cluster-provider"

func (k *ClusterProvider) Provide() (map[string]trino.ClusterInfo, error) {

	result, cached := k.cache.Get(cacheKey)
	if cached {
		return result.(map[string]trino.ClusterInfo), nil
	}

	ctx := context.TODO()

	coordinators := make(map[string]trino.ClusterInfo)

	namespaces, err := k.k8sClient.CoreV1().Namespaces().List(ctx, v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, ns := range namespaces.Items {
		services, err := k.k8sClient.CoreV1().Services(ns.Name).List(ctx, v1.ListOptions{
			LabelSelector: k.svcLabelSelector,
		})

		if err != nil {
			return nil, err
		}

		for _, svc := range services.Items {
			servicePort, err := portByName(svc.Spec.Ports, svcPortName)
			if err != nil {
				logrus.Debug(err)
				continue
			}

			svcUrl, err := url.Parse(fmt.Sprintf("http://%s.%s.svc.%s:%d", svc.Name, svc.Namespace, k.clusterDomain, servicePort.Port))
			if err != nil {
				return nil, err
			}

			name := fmt.Sprintf("%s,%s", svc.Namespace, svc.Name)

			logrus.Infof("discovered service %s", svc.Name)
			coordinators[name] = trino.ClusterInfo{
				Host: svcUrl.String(),
			}
		}
	}

	k.cache.Set(cacheKey, coordinators, 30*time.Minute)
	return coordinators, nil
}

func portByName(ports []v12.ServicePort, name string) (v12.ServicePort, error) {
	for _, port := range ports {
		if port.Name == name {
			return port, nil
		}
	}

	return v12.ServicePort{}, fmt.Errorf("no port with name %s found", name)
}
