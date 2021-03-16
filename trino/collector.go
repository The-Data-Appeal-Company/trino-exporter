package trino

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"time"
)

var namespace = "trino_cluster"

var (
	runningQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_queries"),
		"Running requests of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	blockedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "blocked_queries"),
		"Blocked queries of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	queuedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queued_queries"),
		"Queued queries of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	activeWorkers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "active_workers"),
		"Active workers of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	runningDrivers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_drivers"),
		"Running drivers of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	reservedMemory = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "reserved_memory"),
		"Reserved memory of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	totalInputRows = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_rows"),
		"Total input rows of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	totalInputBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_bytes"),
		"Total input bytes of the trino cluster.",
		[]string{"cluster_name"}, nil,
	)
	totalCpuTimeSecs = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_cpu_time_secs"),
		"Total cpu time of the trino cluster.",
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
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
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

	for name, cluster := range clusters {

		response, err := c.statisticsFromCluster(cluster)
		labelValues := []string{name}

		if err != nil {
			logrus.Error(err)
			out <- prometheus.MustNewConstMetric(up, prometheus.GaugeValue, 0, labelValues...)
			continue
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

func (c Collector) statisticsFromCluster(cluster ClusterInfo) (Response, error) {
	return c.readClusterStats(cluster)
}

func (c Collector) readClusterStats(cluster ClusterInfo) (Response, error) {
	login, err := c.login(cluster)
	if err != nil {
		return Response{}, err
	}

	apiStatsUrl := fmt.Sprintf("%s%s", cluster.Host, "/ui/api/stats")
	req, err := http.NewRequest("GET", apiStatsUrl, nil)
	if err != nil {
		return Response{}, err
	}

	req.Header.Set("Cookie", login)

	resp, err := c.client.Do(req)
	if err != nil {
		return Response{}, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Response{}, err
	}

	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		return Response{}, err
	}

	return response, nil
}

func (c Collector) login(cluster ClusterInfo) (string, error) {
	loginUrl := fmt.Sprintf("%s%s", cluster.Host, "/ui/login")
	const contentType = "application/x-www-form-urlencoded"
	const userName = "exporter"
	body := bytes.NewBuffer([]byte(fmt.Sprintf("username=%s&password=&redirectPath=", userName)))
	resp, err := c.client.Post(loginUrl, contentType, body)
	if err != nil {
		return "", err
	}

	cookie := resp.Header.Get("Set-Cookie")

	if cookie == "" {
		return "", errors.New("no Set-Cookie header present in response")
	}

	return cookie, nil
}

type Response struct {
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
