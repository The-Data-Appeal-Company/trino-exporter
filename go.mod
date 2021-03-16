module trino-exporter

go 1.16

require (
	github.com/aws/aws-sdk-go v1.33.5
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/prometheus/client_golang v1.7.1
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.5.1
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e // indirect
	k8s.io/api v0.19.3
	k8s.io/apimachinery v0.19.3
	k8s.io/client-go v0.19.3
	k8s.io/klog v1.0.0 // indirect
	k8s.io/utils v0.0.0-20201027101359-01387209bb0d // indirect
)
