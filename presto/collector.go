package presto

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"time"
)

var namespace = "presto_cluster"

var (
	runningQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_queries"),
		"Running requests of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	blockedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "blocked_queries"),
		"Blocked queries of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	queuedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queued_queries"),
		"Queued queries of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	activeWorkers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "active_workers"),
		"Active workers of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	runningDrivers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_drivers"),
		"Running drivers of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	reservedMemory = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "reserved_memory"),
		"Reserved memory of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	totalInputRows = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_rows"),
		"Total input rows of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	totalInputBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_bytes"),
		"Total input bytes of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	totalCpuTimeSecs = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_cpu_time_secs"),
		"Total cpu time of the presto cluster.",
		[]string{"cluster_name"}, nil,
	)
	up = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "up"),
		"Presto health check.",
		[]string{"cluster_name"}, nil,
	)
)

type Collector struct {
	client          *http.Client
	clusterProvider ClusterProvider
}

func NewCollector(clusterProvider ClusterProvider) Collector {
	return Collector{
		clusterProvider: clusterProvider,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- runningQueries
	ch <- blockedQueries
	ch <- queuedQueries
	ch <- activeWorkers
	ch <- runningDrivers
	ch <- reservedMemory
	ch <- totalInputRows
	ch <- totalInputBytes
	ch <- totalCpuTimeSecs
	ch <- up
}

func (c Collector) Collect(out chan<- prometheus.Metric) {
	clusters, err := c.clusterProvider.Provide()
	if err != nil {
		logrus.Errorf("%s", err)
		return
	}

	for name, hostPort := range clusters {

		url := fmt.Sprintf("http://%s/v1/cluster", hostPort)
		resp, err := c.client.Get(url)

		labelValues := []string{name}

		if err != nil {
			logrus.Error(err)
			out <- prometheus.MustNewConstMetric(up, prometheus.GaugeValue, 0, labelValues...)
			return
		}

		if resp.StatusCode != 200 {
			logrus.Errorf("unexpected status code %d != 200", resp.StatusCode)
			out <- prometheus.MustNewConstMetric(up, prometheus.GaugeValue, 0, labelValues...)
			return
		}

		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logrus.Error(err)
			return
		}

		var response Response
		err = json.Unmarshal(body, &response)
		if err != nil {
			logrus.Error(err)
			return
		}

		out <- prometheus.MustNewConstMetric(runningQueries, prometheus.GaugeValue, response.RunningQueries, labelValues...)
		out <- prometheus.MustNewConstMetric(blockedQueries, prometheus.GaugeValue, response.BlockedQueries, labelValues...)
		out <- prometheus.MustNewConstMetric(queuedQueries, prometheus.GaugeValue, response.QueuedQueries, labelValues...)
		out <- prometheus.MustNewConstMetric(activeWorkers, prometheus.GaugeValue, response.ActiveWorkers, labelValues...)
		out <- prometheus.MustNewConstMetric(runningDrivers, prometheus.GaugeValue, response.RunningDrivers, labelValues...)
		out <- prometheus.MustNewConstMetric(reservedMemory, prometheus.GaugeValue, response.ReservedMemory, labelValues...)
		out <- prometheus.MustNewConstMetric(totalInputRows, prometheus.GaugeValue, response.TotalInputRows, labelValues...)
		out <- prometheus.MustNewConstMetric(totalInputBytes, prometheus.GaugeValue, response.TotalInputBytes, labelValues...)
		out <- prometheus.MustNewConstMetric(totalCpuTimeSecs, prometheus.GaugeValue, response.TotalCpuTimeSecs, labelValues...)
		out <- prometheus.MustNewConstMetric(up, prometheus.GaugeValue, 1, labelValues...)
	}
}

type Response struct {
	uri              string
	RunningQueries   float64 `json:"runningQueries"`
	BlockedQueries   float64 `json:"blockedQueries"`
	QueuedQueries    float64 `json:"queuedQueries"`
	ActiveWorkers    float64 `json:"activeWorkers"`
	RunningDrivers   float64 `json:"runningDrivers"`
	ReservedMemory   float64 `json:"reservedMemory"`
	TotalInputRows   float64 `json:"totalInputRows"`
	TotalInputBytes  float64 `json:"totalInputBytes"`
	TotalCpuTimeSecs float64 `json:"totalCpuTimeSecs"`
}
