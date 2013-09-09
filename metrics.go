package main

import (
    "github.com/rcrowley/go-metrics"
    "log"
    "time"
)

var appMetrics = NewMetrics()

type Metrics interface {
    TimePusher(f func())
    MarkFailedPusherCount()
    TimeLogPartProcessing(f func())
    MarkFailedLogPartCount()
    StartLogging()
    EachMetric(func(string, interface{}))
}
type LiveMetrics struct {
    Registry           *metrics.StandardRegistry
    ProcessTimer       *metrics.StandardTimer
    ProcessFailedCount *metrics.StandardMeter
    PusherTimer        *metrics.StandardTimer
    PusherFailedCount  *metrics.StandardMeter
}

var _ Metrics = &LiveMetrics{}

func NewMetrics() Metrics {
    registry := metrics.NewRegistry()

    processTimer := metrics.NewTimer()
    registry.Register("logs.process_log_part", processTimer)

    processFailedCount := metrics.NewMeter()
    registry.Register("logs.process_log_part.failed", processFailedCount)

    pusherTimer := metrics.NewTimer()
    registry.Register("logs.process_log_part.pusher", pusherTimer)

    pusherFailedCount := metrics.NewMeter()
    registry.Register("logs.process_log_part.pusher.failed", pusherFailedCount)

    return &LiveMetrics{registry, processTimer, processFailedCount, pusherTimer, pusherFailedCount}
}

func (m *LiveMetrics) TimePusher(f func()) {
    // m.PusherTimer.Time(f)
    f()
}

func (m *LiveMetrics) MarkFailedPusherCount() {
    // m.PusherFailedCount.Mark(1)
}

func (m *LiveMetrics) TimeLogPartProcessing(f func()) {
    // m.ProcessTimer.Time(f)
    f()
}

func (m *LiveMetrics) MarkFailedLogPartCount() {
    // m.ProcessFailedCount.Mark(1)
}

func (m *LiveMetrics) StartLogging() {
    // go func() {
    //     for _ = range time.Tick(time.Duration(60) * time.Second) {
    //         logMetrics(m)
    //     }
    // }()
}

func (m *LiveMetrics) EachMetric(f func(string, interface{})) {
    m.Registry.Each(f)
}

func logMetrics(m Metrics) {
    now := time.Now().Unix()
    m.EachMetric(func(name string, i interface{}) {
        switch m := i.(type) {
        case metrics.Counter:
            log.Printf("metriks: time=%d name=%s type=count count=%d\n", now, name, m.Count())
        case metrics.Gauge:
            log.Printf("metriks: time=%d name=%s type=gauge value=%d\n", now, name, m.Value())
        case metrics.Healthcheck:
            m.Check()
            log.Printf("metriks: time=%d name=%s type=healthcheck error=%v\n", now, name, m.Error())
        case metrics.Histogram:
            ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
            log.Printf("metriks: time=%d name=%s type=histogram count=%d min=%d max=%d mean=%f stddev=%f median=%f 95th_percentile=%f 99th_percentile=%f\n", now, name, m.Count(), int64NanoToSeconds(m.Min()), int64NanoToSeconds(m.Max()), float64NanoToSeconds(m.Mean()), float64NanoToSeconds(m.StdDev()), float64NanoToSeconds(ps[0]), float64NanoToSeconds(ps[2]), float64NanoToSeconds(ps[3]))
        case metrics.Meter:
            log.Printf("metriks: time=%d name=%s type=meter count=%d one_minute_rate=%f five_minute_rate=%f fifteen_minute_rate=%f mean_rate=%f\n", now, name, m.Count(), m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean())
        case metrics.Timer:
            ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
            log.Printf("metriks: time=%d name=%s type=timer count=%d one_minute_rate=%f five_minute_rate=%f fifteen_minute_rate=%f mean_rate=%f min=%f max=%f mean=%f stddev=%f median=%f 95th_percentile=%f 99th_percentile=%f\n", now, name, m.Count(), m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean(), int64NanoToSeconds(m.Min()), int64NanoToSeconds(m.Max()), float64NanoToSeconds(m.Mean()), float64NanoToSeconds(m.StdDev()), float64NanoToSeconds(ps[0]), float64NanoToSeconds(ps[2]), float64NanoToSeconds(ps[3]))
        }
    })
}

func int64NanoToSeconds(d int64) float64 {
    return float64(d) / float64(time.Second)
}

func float64NanoToSeconds(d float64) float64 {
    return d / float64(time.Second)
}
