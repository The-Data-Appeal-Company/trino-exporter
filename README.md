# trino-exporter 
[![Go Report Card](https://goreportcard.com/badge/github.com/The-Data-Appeal-Company/trino-exporter)](https://goreportcard.com/report/github.com/The-Data-Appeal-Company/trino-exporter)
[![MicroBadger Size](https://img.shields.io/microbadger/image-size/The-Data-Appeal-Company/trino-exporter)](https://cloud.docker.com/u/garugaru/repository/docker/garugaru/trino-exporter)


Prometheus exporter for trino with aws auto discovery 

### usage

you can scrape metrics on **<exporter-host>:9999/metrics**

```
trino-exporter --cluster=trino.cluster0:8889,trino.cluster1:8889
```

### usage (aws emr auto-discovery)
```
trino-exporter --aws-autodiscovery=true
```

## Exported metrics 
**Each metric has a label called *cluster_name***

* trino_cluster_up 
* trino_cluster_active_workers
* trino_cluster_blocked_queries          
* trino_cluster_queued_queries           
* trino_cluster_reserved_memory           
* trino_cluster_running_drivers          
* trino_cluster_running_queries           
* trino_cluster_total_cpu_time_secs          
* trino_cluster_total_input_bytes          
* trino_cluster_total_input_rows           
