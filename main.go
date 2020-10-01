package main

import (
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"presto-exporter/aws"
	"presto-exporter/presto"
	"strings"
)

func main() {
	addr := flag.String("addr", "0.0.0.0", "web server bind address")
	port := flag.Int("port", 9999, "web server port")
	metricsPath := flag.String("path", "/metrics", "exporter metrics path")
	awsAutoDiscovery := flag.Bool("aws-autodiscovery", false, "autodiscover cluster in aws (may require permissions)")
	clustersRaw := flag.String("cluster", "127.0.0.1:8889", "clusters to monitor separated by ','")

	flag.Parse()

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	registry := prometheus.NewRegistry()

	var clusterProvider presto.ClusterProvider = FlagClusterProvider{flag: *clustersRaw}

	if *awsAutoDiscovery {
		clusterProvider = aws.NewClusterProvider()
	}

	registry.MustRegister(presto.NewCollector(clusterProvider))

	http.Handle(*metricsPath, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	bind := fmt.Sprintf("%s:%d", *addr, *port)

	log.Infof("started metrics server on %s", bind)

	if err := http.ListenAndServe(bind, nil); err != nil {
		panic(err)
	}
}

type FlagClusterProvider struct {
	flag string
}

func (f FlagClusterProvider) Provide() (map[string]presto.ClusterInfo, error) {
	cnt := len(f.flag)
	clustersToMonitor := make(map[string]presto.ClusterInfo, cnt)
	if cnt != 0 {
		clusters := strings.Split(f.flag, ",")
		for _, c := range clusters {
			clustersToMonitor[c] = presto.ClusterInfo{
				Host:         c,
				Distribution: presto.DistDb,
			}
		}
	}
	return clustersToMonitor, nil
}
