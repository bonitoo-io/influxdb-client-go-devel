package log

import "log"

type Logger struct {
	debugLevel uint
}

func (l *Logger) SetDebugLevel(debugLevel uint) {
	l.debugLevel = debugLevel
}

func (l *Logger) DebugF(format string, v ...interface{}) {
	if l.debugLevel > 2 {
		log.Print("[D]! ")
		log.Printf(format, v...)
	}
}
func (l *Logger) DebugLn(msg string) {
	if l.debugLevel > 2 {
		log.Print("[D]! ")
		log.Println(msg)
	}
}

func (l *Logger) InfoF(format string, v ...interface{}) {
	if l.debugLevel > 1 {
		log.Print("[I]! ")
		log.Printf(format, v...)
	}
}
func (l *Logger) InfoLn(msg string) {
	if l.debugLevel > 1 {
		log.Print("[I]! ")
		log.Println(msg)
	}
}

func (l *Logger) WarnF(format string, v ...interface{}) {
	if l.debugLevel > 0 {
		log.Print("[W]! ")
		log.Printf(format, v...)
	}
}
func (l *Logger) WarnLn(msg string) {
	if l.debugLevel > 0 {
		log.Print("[W]! ")
		log.Println(msg)
	}
}

func (l *Logger) ErrorF(format string, v ...interface{}) {
	log.Print("[E]! ")
	log.Printf(format, v...)
}

func (l *Logger) ErrorLn(msg string) {
	log.Print("[E]! ")
	log.Println(msg)
}
