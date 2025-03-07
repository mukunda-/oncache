//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"time"

	"go.mukunda.com/oncache/oncrypt"
)

// This process listens for connections and spawns goroutines for handling clients. When
// a client connects, it registers them as a peer.
func (oc *Oncache) listenerProcess() {
	// If anything causes an unexpected panic, try to restart.
	defer oc.onProcessCompleted("listenerProcess", func() { oc.listenerProcess() })

restart:
	select {
	case <-oc.stopSignal.C:
		return
	default:
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", oc.listenPort))
	if err != nil {
		logError(fmt.Sprintf("Failed to start listener: %s; retrying in 60 seconds.", err))
		time.Sleep(60 * time.Second)
		goto restart
	}

	logInfo(fmt.Sprintf("Listening on port %d", oc.listenPort))
	oc.initialListenerStarted.raise()
	listenerExited := make(chan struct{})

	go func() {
		defer close(listenerExited)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if err != net.ErrClosed {
					logError(fmt.Sprintf("Listener accept failed: %s.", err))
				} else {
					logInfo("Listener closed.")
				}
				return
			}

			go oc.handleConnection(conn)
		}
	}()

	for {
		select {
		case <-listenerExited:
			logError("Listener exited unexpectedly; restarting listener.")
			goto restart
		case <-oc.stopSignal.C:
			// Oncache is shutting down.
			listener.Close()
			<-listenerExited // Wait for above goroutine to exit before releasing resources.
			return
		}
	}
}

// Handle a connection. Decrypt the stream, register the client, and monitor for messages.
// Complete messages are delivered to the message handler.
func (oc *Oncache) handleConnection(conn net.Conn) {
	oc.activeWork.Add(1) // removed in panic handler func
	fromAddress := conn.RemoteAddr().String()
	exiting := make(chan struct{})
	logDebug(fmt.Sprintf("Accepted connection from %s.", fromAddress))

	// Upon shutdown, close the connection.
	go func() {
		select {
		case <-oc.stopSignal.C:
			// System is shutting down, close the connection.
			conn.Close()
		case <-exiting:
			// Connection has closed.
		}
	}()
	defer close(exiting) // Stop the above goroutine upon exiting this function.

	defer conn.Close() // Make sure the connection is closed if we return from this function.
	defer func() {
		r := recover()
		if r != nil {
			logError(
				fmt.Sprintf("[%s] Connection handler panicked: %v; %s",
					fromAddress, r, string(debug.Stack())))
		}
		oc.activeWork.Done()
	}()

	decrypter, err := oncrypt.DecryptStream(oc.encryptionKey, conn)
	if err != nil {
		logError(fmt.Sprintf("Failed to decrypt stream: %s", err))
		return
	}

	scanner := bufio.NewScanner(decrypter)
	if !scanner.Scan() {
		logError(fmt.Sprintf("Failed to read firstline from %s: %v", fromAddress, scanner.Err()))
		return
	}
	firstline := scanner.Text()

	fields := strings.Fields(firstline)
	if len(fields) < 2 {
		logError(fmt.Sprintf("Failed to parse conn header from %s: %s", fromAddress, err))
		return
	}

	if fields[0] != "v1" {
		logError(fmt.Sprintf("Unsupported protocol from %s: %s", fromAddress, fields[0]))
		return
	}

	host := fields[1]
	if fromAddress != host {
		fromAddress = fromAddress + "/" + host
	}

	for scanner.Scan() {
		message := scanner.Text()
		logDebug(fmt.Sprintf("[%s]: %s", fromAddress, message))
		oc.handleMessageReceived(host, message)
	}

	if err := scanner.Err(); err != nil && !errors.Is(scanner.Err(), io.EOF) {
		logError(fmt.Sprintf("[%s] Connection closed with error: %s", fromAddress, scanner.Err().Error()))
	}
}
