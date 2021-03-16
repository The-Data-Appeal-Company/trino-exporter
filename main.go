package main

import (
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strings"
	"trino-exporter/aws"
	k8s "trino-exporter/kubernetes"
	"trino-exporter/trino"
)

func main() {
	addr := flag.String("addr", "0.0.0.0", "web server bind address")
	port := flag.Int("port", 9999, "web server port")
	metricsPath := flag.String("path", "/metrics", "exporter metrics path")
	awsAutoDiscovery := flag.Bool("aws-autodiscovery", false, "autodiscover cluster in aws (may require permissions)")
	k8sAutoDiscovery := flag.Bool("k8s-autodiscovery", false, "autodiscover cluster in k8s (may require permissions)")
	clustersRaw := flag.String("cluster", "", "clusters to monitor separated by ',' eg: http://127.0.0.1:8889,http://127.0.0.1:8888")

	flag.Parse()

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	registry := prometheus.NewRegistry()

	var clusterProvider = trino.NewMultiClusterProvider()

	clusterProvider.Add(FlagClusterProvider{flag: *clustersRaw})

	if *awsAutoDiscovery {
		log.Info("enabled aws discovery")
		clusterProvider.Add(aws.NewClusterProvider())
	}

	if *k8sAutoDiscovery {
		log.Info("enabled k8s in cluster discovery")

		provider, err := k8s.NewInClusterProvider("cluster.local")
		if err != nil {
			log.Fatal(err)
		}

		clusterProvider.Add(provider)
	}

	registry.MustRegister(trino.NewCollector(clusterProvider))

	http.Handle(*metricsPath, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	http.Handle("/healthz", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, err := writer.Write([]byte("OK")); err != nil {
			log.Error(err)
		}
	}))

	bind := fmt.Sprintf("%s:%d", *addr, *port)

	log.Infof("started metrics server on %s", bind)

	if err := http.ListenAndServe(bind, nil); err != nil {
		panic(err)
	}
}

type FlagClusterProvider struct {
	flag string
}

func (f FlagClusterProvider) Provide() (map[string]trino.ClusterInfo, error) {
	cnt := len(f.flag)
	clustersToMonitor := make(map[string]trino.ClusterInfo, cnt)
	if cnt != 0 {
		clusters := strings.Split(f.flag, ",")
		for _, c := range clusters {
			clustersToMonitor[c] = trino.ClusterInfo{
				Host: c,
			}
		}
	}
	return clustersToMonitor, nil
}
