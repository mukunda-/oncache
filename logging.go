//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import (
	"fmt"
	"sync"
)

var myLogger LoggerFunc
var logMutex sync.Mutex

type LogLevel int8

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

type LoggerFunc = func(level LogLevel, message string)

func SetLogger(logger LoggerFunc) {
	myLogger = logger
}

func StdOutLogger(level LogLevel, message string) {
	switch level {
	case LogLevelDebug:
		fmt.Println("[DEBG] " + message)
	case LogLevelInfo:
		fmt.Println("[INFO] " + message)
	case LogLevelWarn:
		fmt.Println("[WARN] " + message)
	case LogLevelError:
		fmt.Println("[ERRO] " + message)
	}
}

func log(level LogLevel, message string) {
	if myLogger != nil {
		logMutex.Lock()
		defer logMutex.Unlock()
		myLogger(level, message)
	}
}

func logDebug(message string) {
	log(LogLevelDebug, message)
}

func logInfo(message string) {
	log(LogLevelInfo, message)
}

func logError(message string) {
	log(LogLevelError, message)
}
