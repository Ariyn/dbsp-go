package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type collectorSet struct {
	pipelineBatches       prometheus.Counter
	pipelineRowsTotal     *prometheus.CounterVec
	pipelineBatchRows     *prometheus.HistogramVec
	pipelineStageDuration *prometheus.HistogramVec
	operatorAppliesTotal  *prometheus.CounterVec
	operatorRowsTotal     *prometheus.CounterVec
	operatorAppendTotal   *prometheus.CounterVec
	operatorOutputTotal   *prometheus.CounterVec
	operatorStateEntries  *prometheus.GaugeVec
}

var (
	defaultCollectorsOnce sync.Once
	defaultCollectors     *collectorSet
)

func Handler() http.Handler {
	ensureDefaultCollectors()
	return promhttp.Handler()
}

func ObservePipelineBatch(inRows, outRows int, nextDur, execDur, sinkDur, ackDur, totalDur time.Duration) {
	collectors := ensureDefaultCollectors()
	collectors.observePipelineBatch(inRows, outRows, nextDur, execDur, sinkDur, ackDur, totalDur)
}

func ObserveOperatorBatch(label string, inRows, outRows, distinctOutRows, appendHits, appendMisses int) {
	collectors := ensureDefaultCollectors()
	collectors.observeOperatorBatch(label, inRows, outRows, distinctOutRows, appendHits, appendMisses)
}

func ObserveOperatorState(label string, stateEntries int) {
	collectors := ensureDefaultCollectors()
	collectors.observeOperatorState(label, stateEntries)
}

func ensureDefaultCollectors() *collectorSet {
	defaultCollectorsOnce.Do(func() {
		defaultCollectors = newCollectorSet(prometheus.DefaultRegisterer)
	})
	return defaultCollectors
}

func newCollectorSet(registerer prometheus.Registerer) *collectorSet {
	collectors := &collectorSet{
		pipelineBatches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dbsp",
			Subsystem: "pipeline",
			Name:      "batches_total",
			Help:      "Total number of processed pipeline batches.",
		}),
		pipelineRowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dbsp",
			Subsystem: "pipeline",
			Name:      "rows_total",
			Help:      "Total number of pipeline rows by direction.",
		}, []string{"kind"}),
		pipelineBatchRows: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dbsp",
			Subsystem: "pipeline",
			Name:      "batch_rows",
			Help:      "Distribution of pipeline batch sizes by direction.",
			Buckets:   []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000},
		}, []string{"kind"}),
		pipelineStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dbsp",
			Subsystem: "pipeline",
			Name:      "stage_duration_seconds",
			Help:      "Pipeline stage duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"stage"}),
		operatorAppliesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dbsp",
			Subsystem: "operator",
			Name:      "applies_total",
			Help:      "Total number of operator apply calls.",
		}, []string{"operator"}),
		operatorRowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dbsp",
			Subsystem: "operator",
			Name:      "rows_total",
			Help:      "Total number of operator input and output rows.",
		}, []string{"operator", "kind"}),
		operatorAppendTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dbsp",
			Subsystem: "operator",
			Name:      "append_total",
			Help:      "Total number of append fast-path hits and misses.",
		}, []string{"operator", "result"}),
		operatorOutputTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dbsp",
			Subsystem: "operator",
			Name:      "output_rows_total",
			Help:      "Total number of distinct and repeated operator output rows.",
		}, []string{"operator", "kind"}),
		operatorStateEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "dbsp",
			Subsystem: "operator",
			Name:      "state_entries",
			Help:      "Current number of retained state entries per operator.",
		}, []string{"operator"}),
	}
	registerer.MustRegister(
		collectors.pipelineBatches,
		collectors.pipelineRowsTotal,
		collectors.pipelineBatchRows,
		collectors.pipelineStageDuration,
		collectors.operatorAppliesTotal,
		collectors.operatorRowsTotal,
		collectors.operatorAppendTotal,
		collectors.operatorOutputTotal,
		collectors.operatorStateEntries,
	)
	return collectors
}

func (c *collectorSet) observePipelineBatch(inRows, outRows int, nextDur, execDur, sinkDur, ackDur, totalDur time.Duration) {
	c.pipelineBatches.Inc()
	c.pipelineRowsTotal.WithLabelValues("input").Add(float64(inRows))
	c.pipelineRowsTotal.WithLabelValues("output").Add(float64(outRows))
	c.pipelineBatchRows.WithLabelValues("input").Observe(float64(inRows))
	c.pipelineBatchRows.WithLabelValues("output").Observe(float64(outRows))
	c.pipelineStageDuration.WithLabelValues("next").Observe(nextDur.Seconds())
	c.pipelineStageDuration.WithLabelValues("execute").Observe(execDur.Seconds())
	c.pipelineStageDuration.WithLabelValues("sink").Observe(sinkDur.Seconds())
	c.pipelineStageDuration.WithLabelValues("ack").Observe(ackDur.Seconds())
	c.pipelineStageDuration.WithLabelValues("total").Observe(totalDur.Seconds())
}

func (c *collectorSet) observeOperatorBatch(label string, inRows, outRows, distinctOutRows, appendHits, appendMisses int) {
	repeatedOutRows := outRows - distinctOutRows
	if repeatedOutRows < 0 {
		repeatedOutRows = 0
	}
	c.operatorAppliesTotal.WithLabelValues(label).Inc()
	c.operatorRowsTotal.WithLabelValues(label, "input").Add(float64(inRows))
	c.operatorRowsTotal.WithLabelValues(label, "output").Add(float64(outRows))
	c.operatorOutputTotal.WithLabelValues(label, "distinct").Add(float64(distinctOutRows))
	c.operatorOutputTotal.WithLabelValues(label, "repeated").Add(float64(repeatedOutRows))
	if appendHits > 0 {
		c.operatorAppendTotal.WithLabelValues(label, "hit").Add(float64(appendHits))
	}
	if appendMisses > 0 {
		c.operatorAppendTotal.WithLabelValues(label, "miss").Add(float64(appendMisses))
	}
}

func (c *collectorSet) observeOperatorState(label string, stateEntries int) {
	if stateEntries < 0 {
		stateEntries = 0
	}
	c.operatorStateEntries.WithLabelValues(label).Set(float64(stateEntries))
}
