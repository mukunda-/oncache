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

// Define your own LoggerFunc and pass it to SetLogger to receive log messages from the
// system.
type LoggerFunc = func(level LogLevel, message string)

// Enable logging by calling this function with a log callback. Provide `nil` to disable
// logging.
func SetLogger(logger LoggerFunc) {
	myLogger = logger
}

// This is a simple logger that writes everything to stdout.
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

// Main logging function. Forwards a log message to the registered log handler. Does
// nothing if no logger is set up.
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

func logWarn(message string) {
	log(LogLevelWarn, message)
}

func logError(message string) {
	log(LogLevelError, message)
}
