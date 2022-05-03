package logger

import "encoding/json"

type StandardLogger interface {
	Printf(format string, v ...interface{})
}

type LoggerPublisher struct {
	Logger StandardLogger
}

func (l LoggerPublisher) Publish(b []byte) {
	l.Logger.Printf("%v\n", json.RawMessage(b))
}
