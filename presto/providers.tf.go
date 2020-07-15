package presto

type ClusterProvider interface {
	Provide() (map[string]string, error)
}
