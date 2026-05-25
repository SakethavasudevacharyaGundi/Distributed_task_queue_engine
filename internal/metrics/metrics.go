package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)
var QueueDepth =prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "task_queue_depth",
	Help: "Current depth of the task queue",
})
var TasksProcessed = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "tasks_processed_total",
	Help: "Total number of tasks processed",
})
var TasksFailed = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "tasks_failed_total",
	Help: "Total number of tasks failed",
})
var RetryCount = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "task_retry_count_total",
	Help: "Total number of task retries",
})
var DLQsize = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "dead_letter_queue_size",
	Help: "Current size of the dead letter queue",
})
var TaskDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "task_processing_duration_seconds",
	Help:    "Duration of task processing in seconds",
})
var ProcessingTime = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "task_processing_time_seconds",
	Help:    "Time taken to process tasks in seconds",
})
func Init() {
	prometheus.MustRegister(QueueDepth)
	prometheus.MustRegister(TasksProcessed)
	prometheus.MustRegister(TasksFailed)
	prometheus.MustRegister(RetryCount)
	prometheus.MustRegister(DLQsize)
	prometheus.MustRegister(TaskDuration)
	prometheus.MustRegister(ProcessingTime)
}