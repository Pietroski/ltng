package badgerdb_logger

type (
	SilentLogger struct{}
)

func NewBadgerDBSilentLogger() *SilentLogger {
	return &SilentLogger{}
}

func (l *SilentLogger) Errorf(format string, v ...interface{})   { return }
func (l *SilentLogger) Infof(format string, v ...interface{})    { return }
func (l *SilentLogger) Warningf(format string, v ...interface{}) { return }
func (l *SilentLogger) Debugf(format string, v ...interface{})   { return }
