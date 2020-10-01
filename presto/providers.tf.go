package presto

type Distribution string

const (
	DistSql Distribution = "sql"
	DistDb  Distribution = "db"
)

type ClusterInfo struct {
	Host         string
	Distribution Distribution
}

type ClusterProvider interface {
	Provide() (map[string]ClusterInfo, error)
}
