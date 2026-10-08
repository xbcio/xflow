package metrics

import "time"

// Consumer-queue metric names.
const (
	metricQueueDepth            = "xflow_queue_depth"
	metricQueueOldestPendingAge = "xflow_queue_oldest_pending_age_seconds"
)

// queueStatsObserver mirrors the consumer-side queue statistics observer
// (queue.StatsObserver via the distributed backend's alias). It is declared
// with primitive parameters so this package does not import backend providers.
type queueStatsObserver interface {
	OnQueueStats(queue string, pending, active, retry int, oldestPendingAge time.Duration)
}

// QueueStatsMetrics observes periodic queue-depth samples from a task
// consumer. It is the broker-side half of the delivery-residency signal: it
// can show a backlog while tasks are still queued, including a fully stalled
// consumer, which no consumer-side observation can report.
type QueueStatsMetrics struct {
	Metrics *Metrics
}

func NewQueueStatsMetrics(metrics *Metrics) QueueStatsMetrics {
	return QueueStatsMetrics{Metrics: metrics}
}

// OnQueueStats records one sample per queue. Only pending, active and retry
// are recorded: scheduled tasks are legitimately parked (timers, suspend
// wakeups, retry backoff) and counting them as backlog would make a healthy
// queue look congested.
func (q QueueStatsMetrics) OnQueueStats(queue string, pending, active, retry int, oldestPendingAge time.Duration) {
	q.Metrics.Set(metricQueueDepth, map[string]string{"queue": queue, "state": "pending"}, float64(pending))
	q.Metrics.Set(metricQueueDepth, map[string]string{"queue": queue, "state": "active"}, float64(active))
	q.Metrics.Set(metricQueueDepth, map[string]string{"queue": queue, "state": "retry"}, float64(retry))
	q.Metrics.Set(metricQueueOldestPendingAge, map[string]string{"queue": queue}, oldestPendingAge.Seconds())
}

var _ queueStatsObserver = QueueStatsMetrics{}
