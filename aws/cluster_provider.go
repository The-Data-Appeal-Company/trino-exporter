package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/patrickmn/go-cache"
	"strings"
	"time"
)

type ClusterProvider struct {
	client *emr.EMR
	cache  *cache.Cache
}

func NewClusterProvider() *ClusterProvider {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	return &ClusterProvider{
		client: emr.New(sess),
		cache:  cache.New(60*time.Minute, 24*time.Hour),
	}
}

const cacheKey = "master"

func (c *ClusterProvider) Provide() (map[string]string, error) {
	result, cached := c.cache.Get(cacheKey)
	if cached {
		return result.(map[string]string), nil
	}

	masters, err := c.listTargetMasters(context.Background())
	if err != nil {
		return nil, err
	}

	c.cache.Set(cacheKey, masters, 30*time.Minute)

	return masters, nil
}

func (c *ClusterProvider) listTargetMasters(ctx context.Context) (map[string]string, error) {

	clusterWithMaster := make(map[string]string)

	clusters, err := c.listTargetClusters(ctx)

	if err != nil {
		return nil, err
	}

	for _, cluster := range clusters {
		master, err := c.getClusterMasterInstance(cluster)
		if err != nil {
			return nil, err
		}

		clusterWithMaster[*cluster.Name] = fmt.Sprintf("%s:8889", master)
	}

	return clusterWithMaster, nil
}

func (c *ClusterProvider) listTargetClusters(ctx context.Context) ([]*emr.ClusterSummary, error) {
	req := &emr.ListClustersInput{
		ClusterStates: aws.StringSlice([]string{"WAITING"}),
	}

	clusters := make([]*emr.ClusterSummary, 0)
	err := c.client.ListClustersPagesWithContext(ctx, req, func(output *emr.ListClustersOutput, b bool) bool {

		for _, cluster := range output.Clusters {

			descr, _ := c.client.DescribeCluster(&emr.DescribeClusterInput{
				ClusterId: cluster.Id,
			})

			if !hasPrestoInstalled(descr) {
				continue
			}

			clusters = append(clusters, output.Clusters...)

		}
		return true
	})

	return clusters, err
}

func (c *ClusterProvider) getClusterMasterInstance(cluster *emr.ClusterSummary) (string, error) {
	instanceGroups, err := c.client.ListInstanceGroups(&emr.ListInstanceGroupsInput{
		ClusterId: cluster.Id,
	})

	if err != nil {
		return "", err
	}

	for _, group := range instanceGroups.InstanceGroups {
		if *group.InstanceGroupType != emr.InstanceGroupTypeMaster {
			continue
		}

		instances, err := c.client.ListInstances(&emr.ListInstancesInput{
			ClusterId:       cluster.Id,
			InstanceGroupId: group.Id,
		})

		if err != nil {
			return "", err
		}

		if len(instances.Instances) == 0 {
			return "", fmt.Errorf("no master instances found for cluster %s", *cluster.Id)
		}

		return *instances.Instances[0].PrivateIpAddress, nil
	}

	return "", fmt.Errorf("no master intance found for cluster: %s", *cluster.Id)
}

func hasPrestoInstalled(descr *emr.DescribeClusterOutput) bool {
	for _, application := range descr.Cluster.Applications {
		if strings.ToLower(*application.Name) == "presto" {
			return true
		}
	}
	return false
}
