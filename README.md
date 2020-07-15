# presto-exporter 
[![Go Report Card](https://goreportcard.com/badge/github.com/The-Data-Appeal-Company/presto-exporter)](https://goreportcard.com/report/github.com/The-Data-Appeal-Company/presto-exporter)
[![MicroBadger Size](https://img.shields.io/microbadger/image-size/The-Data-Appeal-Company/presto-exporter)](https://cloud.docker.com/u/garugaru/repository/docker/garugaru/presto-exporter)


Prometheus exporter for presto with aws auto discovery 

### usage

you can scrape metrics on **<exporter-host>:9999/metrics**

```
presto-exporter --cluster=presto.cluster0:8889,presto.cluster1:8889
```

### usage (aws emr auto-discovery)
```
presto-exporter --aws-autodiscovery=true
```

## Exported metrics 
**Each metric has a label called *cluster_name***

* presto_cluster_active_workers
* presto_cluster_blocked_queries          
* presto_cluster_queued_queries           
* presto_cluster_reserved_memory           
* presto_cluster_running_drivers          
* presto_cluster_running_queries           
* presto_cluster_total_cpu_time_secs          
* presto_cluster_total_input_bytes          
* presto_cluster_total_input_rows           
