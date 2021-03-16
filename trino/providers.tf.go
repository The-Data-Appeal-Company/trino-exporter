package trino

import "fmt"

type ClusterInfo struct {
	Host string
}

type ClusterProvider interface {
	Provide() (map[string]ClusterInfo, error)
}

type MultiClusterProvider struct {
	providers []ClusterProvider
}

func NewMultiClusterProvider() *MultiClusterProvider {
	return &MultiClusterProvider{providers: make([]ClusterProvider, 0)}
}

func (m *MultiClusterProvider) Add(provider ClusterProvider) {
	m.providers = append(m.providers, provider)
}

func (m *MultiClusterProvider) Provide() (map[string]ClusterInfo, error) {
	clusters := make(map[string]ClusterInfo)
	for _, provider := range m.providers {
		providerClusters, err := provider.Provide()
		if err != nil {
			return nil, err
		}

		for name, cluster := range providerClusters {
			_, present := clusters[name]
			if present {
				return nil, fmt.Errorf("duplicated cluster name between providers: %s", name)
			}

			clusters[name] = cluster
		}
	}

	return clusters, nil
}
