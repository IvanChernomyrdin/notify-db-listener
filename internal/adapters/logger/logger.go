package logger

import (
	"github.com/ZhuchkovAA/loglib"
	"github.com/ZhuchkovAA/loglib/config"

	dLog "db_listener/internal/domain/log"
)

type Logger struct {
	Client *loglib.Client
}

func New(grpcAddress, fallbackPath, serviceName string) (*Logger, error) {
	logger, err := loglib.New(config.Config{
		GRPCAddress:  grpcAddress,
		FallbackPath: fallbackPath,
		ServiceName:  serviceName,
	})
	if err != nil {
		return nil, err
	}

	return &Logger{
		Client: logger,
	}, nil
}

func (l *Logger) Debug(message string, fields ...dLog.Field) {
	l.Client.Debug(message, fieldsMapper(fields)...)
}

func (l *Logger) Info(message string, fields ...dLog.Field) {
	l.Client.Info(message, fieldsMapper(fields)...)
}

func (l *Logger) Warn(message string, fields ...dLog.Field) {
	l.Client.Warn(message, fieldsMapper(fields)...)
}

func (l *Logger) Error(message string, fields ...dLog.Field) {
	l.Client.Error(message, fieldsMapper(fields)...)
}

func fieldMapper(dField dLog.Field) loglib.Field {
	return loglib.Field{
		Key:   dField.Key,
		Value: dField.Value,
	}
}

func fieldsMapper(dFields []dLog.Field) []loglib.Field {
	fields := make([]loglib.Field, len(dFields))
	for i, d := range dFields {
		fields[i] = fieldMapper(d)
	}
	return fields
}
