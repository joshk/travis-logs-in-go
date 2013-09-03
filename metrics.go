package main

import (
    "github.com/rcrowley/go-metrics"
    "log"
    "time"
)

var appMetrics = NewMetrics()

type Metrics struct {
    Registry           *metrics.StandardRegistry
    ProcessTimer       *metrics.StandardTimer
    ProcessFailedCount *metrics.StandardMeter
    PusherTimer        *metrics.StandardTimer
    PusherFailedCount  *metrics.StandardMeter
}

func (m *Metrics) TimePusher(f func()) {
    m.PusherTimer.Time(f)
}

func (m *Metrics) MarkFailedPusherCount() {
    m.PusherFailedCount.Mark(1)
}

func (m *Metrics) TimeLogPartProcessing(f func()) {
    m.ProcessTimer.Time(f)
}

func (m *Metrics) MarkFailedLogPartCount() {
    m.ProcessFailedCount.Mark(1)
}

func NewMetrics() *Metrics {
    registry := metrics.NewRegistry()

    processTimer := metrics.NewTimer()
    registry.Register("logs.process_log_part", processTimer)

    processFailedCount := metrics.NewMeter()
    registry.Register("logs.process_log_part.failed", processFailedCount)

    pusherTimer := metrics.NewTimer()
    registry.Register("logs.process_log_part.pusher", pusherTimer)

    pusherFailedCount := metrics.NewMeter()
    registry.Register("logs.process_log_part.pusher.failed", pusherFailedCount)

    return &Metrics{registry, processTimer, processFailedCount, pusherTimer, pusherFailedCount}
}

func startMetricsLogging(metrics *Metrics, logger *log.Logger) {
    go func() {
        for _ = range time.Tick(time.Duration(60)*time.Second) {
            logMetrics(metrics, logger)
        }
    }()
}

func logMetrics(m *Metrics, l *log.Logger) {
    now := time.Now().Unix()
    m.Registry.Each(func(name string, i interface{}) {
        switch m := i.(type) {
        case metrics.Counter:
            l.Printf("metriks: time=%d name=%s type=count count=%d\n", now, name, m.Count())
        case metrics.Gauge:
            l.Printf("metriks: time=%d name=%s type=gauge value=%d\n", now, name, m.Value())
        case metrics.Healthcheck:
            m.Check()
            l.Printf("metriks: time=%d name=%s type=healthcheck error=%v\n", now, name, m.Error())
        case metrics.Histogram:
            ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
            l.Printf("metriks: time=%d name=%s type=histogram count=%d min=%d max=%d mean=%f stddev=%f median=%f 95th_percentile=%f 99th_percentile=%f\n", now, name, m.Count(), int64NanoToSeconds(m.Min()), int64NanoToSeconds(m.Max()), float64NanoToSeconds(m.Mean()), float64NanoToSeconds(m.StdDev()), float64NanoToSeconds(ps[0]), float64NanoToSeconds(ps[2]), float64NanoToSeconds(ps[3]))
        case metrics.Meter:
            l.Printf("metriks: time=%d name=%s type=meter count=%d one_minute_rate=%f five_minute_rate=%f fifteen_minute_rate=%f mean_rate=%f\n", now, name, m.Count(), m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean())
        case metrics.Timer:
            ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
            l.Printf("metriks: time=%d name=%s type=timer count=%d one_minute_rate=%f five_minute_rate=%f fifteen_minute_rate=%f mean_rate=%f min=%f max=%f mean=%f stddev=%f median=%f 95th_percentile=%f 99th_percentile=%f\n", now, name, m.Count(), m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean(), int64NanoToSeconds(m.Min()), int64NanoToSeconds(m.Max()), float64NanoToSeconds(m.Mean()), float64NanoToSeconds(m.StdDev()), float64NanoToSeconds(ps[0]), float64NanoToSeconds(ps[2]), float64NanoToSeconds(ps[3]))
        }
    })
}

func int64NanoToSeconds(d int64) float64 {
    return float64(d) / float64(time.Second)
}

func float64NanoToSeconds(d float64) float64 {
    return d / float64(time.Second)
}
