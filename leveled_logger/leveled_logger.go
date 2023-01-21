package leveled_logger

import (
  "sync"
  "log"
)

type LogLevel int
const (
  Off LogLevel = iota
  Normal
  Debug
)
type LeveledLogger struct {
  *log.Logger
  level LogLevel
  mutex sync.Mutex
}

func (l *LeveledLogger) SetLevel(level LogLevel) {
  l.level = level
}

// doesn't really make sense in multiple threads though
func (l *LeveledLogger) AutoPrefix(prefix string) func() {
  l.mutex.Lock()
  defer l.mutex.Unlock()

  oldPrefix := l.Prefix()
  l.SetPrefix(prefix + oldPrefix)
  return func() { l.SetPrefix(oldPrefix) }
}

func (l *LeveledLogger) PrintfNormal(format string, args ...any) {
  l.mutex.Lock()
  defer l.mutex.Unlock()

  if l.level >= Normal {
    l.Printf(format, args)
  }
}

func (l *LeveledLogger) PrintfDebug(format string, args ...any) {
  l.mutex.Lock()
  defer l.mutex.Unlock()

  if l.level >= Debug {
    l.Printf(format, args)
  }
}

