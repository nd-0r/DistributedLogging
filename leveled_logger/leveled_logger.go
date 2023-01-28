package leveled_logger

import (
  "runtime"
  "sync"
  "log"
  "path/filepath"
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

  pc, filename, line, _ := runtime.Caller(1)

  if l.level >= Normal {
    l.Printf(
      "%s[%s:%d] :: " + format,
      append(
        []any{runtime.FuncForPC(pc).Name(), filepath.Base(filename), line},
        args...
      )...
    )
  }
}

func (l *LeveledLogger) PrintfDebug(format string, args ...any) {
  l.mutex.Lock()
  defer l.mutex.Unlock()

  pc, filename, line, _ := runtime.Caller(1)

  if l.level >= Debug {
    l.Printf(
      "%s[%s:%d] :: " + format,
      append(
        []any{runtime.FuncForPC(pc).Name(), filepath.Base(filename), line},
        args...
      )...
    )
  }
}

