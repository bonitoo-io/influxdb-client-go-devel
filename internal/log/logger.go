package log

import (
	"fmt"
	"log"
)

type Logger struct {
	debugLevel uint
}

func (l *Logger) SetDebugLevel(debugLevel uint) {
	l.debugLevel = debugLevel
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	if l.debugLevel > 2 {
		log.Print("[D]! ", fmt.Sprintf(format, v...))
	}
}
func (l *Logger) Debug(msg string) {
	if l.debugLevel > 2 {
		log.Print("[D]! ", msg)
	}
}

func (l *Logger) Infof(format string, v ...interface{}) {
	if l.debugLevel > 1 {
		log.Print("[I]! ", fmt.Sprintf(format, v...))
	}
}
func (l *Logger) Info(msg string) {
	if l.debugLevel > 1 {
		log.Print("[I]! ", msg)
	}
}

func (l *Logger) Warnf(format string, v ...interface{}) {
	if l.debugLevel > 0 {
		log.Print("[W]! ", fmt.Sprintf(format, v...))
	}
}
func (l *Logger) Warn(msg string) {
	if l.debugLevel > 0 {
		log.Print("[W]! ", msg)
	}
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	log.Print("[E]! ", fmt.Sprintf(format, v...))
}

func (l *Logger) Error(msg string) {
	log.Print("[E]! ", msg)
}
