package mtr

import (
	"fmt"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
)

const (
	DefaultNamespace = "reader."
	// DefaultAddr is the default address for where the DD agent would be running on a single host machine
	DefaultAddr = "127.0.0.1:8125"
)

type Client interface {
	Timing(name string, value time.Duration, tags map[string]string)
	Incr(name string, tags map[string]string)
	Gauge(name string, value float64, tags map[string]string)
	Count(name string, value int64, tags map[string]string)
	Flush()
}

type statsClient struct {
	client *statsd.Client
	rate   float64
}

func toDatadogTags(tags map[string]string) []string {
	var retTags []string
	for key, val := range tags {
		retTags = append(retTags, fmt.Sprintf("%s:%s", key, val))
	}

	return retTags
}

func (s *statsClient) Flush() {
	_ = s.client.Flush()
}

func (s *statsClient) Count(name string, value int64, tags map[string]string) {
	_ = s.client.Count(name, value, toDatadogTags(tags), s.rate)
}

func (s *statsClient) Timing(name string, value time.Duration, tags map[string]string) {
	_ = s.client.Timing(name, value, toDatadogTags(tags), s.rate)
}

func (s *statsClient) Incr(name string, tags map[string]string) {
	_ = s.client.Incr(name, toDatadogTags(tags), s.rate)
}

func (s *statsClient) Gauge(name string, value float64, tags map[string]string) {
	_ = s.client.Gauge(name, value, toDatadogTags(tags), s.rate)
}
