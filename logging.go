//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

var myLogger LoggerFunc

type LogLevel int8

const (
	LDebug LogLevel = iota
	LInfo
	LWarning
	LError
)

type LoggerFunc = func(level LogLevel, message string)

func SetLogger(logger LoggerFunc) {
	myLogger = logger
}

func log(level LogLevel, message string) {
	if myLogger != nil {
		myLogger(level, message)
	}
}

func logDebug(message string) {
	log(LDebug, message)
}

func logInfo(message string) {
	log(LInfo, message)
}
