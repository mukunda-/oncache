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
	"strings"
	"sync"
	"time"

	"go.mukunda.com/oncache/oncrypt"
)

// This process listens for connections and handles creation of goroutines for receiving
// clients. When connecting, it registers them as peers.
func listenerProcess(wg *sync.WaitGroup) {
	// If anything causes an unexpected panic, try to restart.
	defer catchProcessPanic("listenerProcess", wg, func() { listenerProcess(wg) })

restart:
	if shutdownSignal.raised() {
		return
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", networkPort))
	if err != nil {
		logError(fmt.Sprintf("Failed to start listener: %s; retrying in 60 seconds.", err))
		time.Sleep(60 * time.Second)
		goto restart
	}

	logInfo(fmt.Sprintf("Listening on port %d", networkPort))
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

			go handleConnection(wg, conn)
		}
	}()

	for {
		select {
		case <-listenerExited:
			logError(fmt.Sprintf("Listener exited unexpectedly; restarting listener."))
			goto restart
		case <-shutdownSignal.C:
			listener.Close()
			<-listenerExited
			return
		}
	}
}

// Handle a connection. Decrypt the stream, register the client, and monitor for messages.
// Complete messages are delivered to the message handler.
func handleConnection(wg *sync.WaitGroup, conn net.Conn) {
	wg.Add(1) // removed in panic handler func
	fromAddress := conn.RemoteAddr().String()
	exiting := make(chan struct{})

	// Upon shutdown, close the connection.
	go func() {
		select {
		case <-shutdownSignal.C:
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
			logError(fmt.Sprintf("[%s] Connection handler panicked: %v", fromAddress, r))
		}
		wg.Done()
	}()

	decrypter, err := oncrypt.DecryptStream(encryptionKey, conn)
	if err != nil {
		logError(fmt.Sprintf("Failed to decrypt stream: %s", err))
		return
	}

	reader := bufio.NewReader(decrypter)
	firstline, err := reader.ReadString('\n')
	if err != nil {
		logError(fmt.Sprintf("Failed to read firstline from %s: %s", fromAddress, err))
		return
	}

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

	scanner := bufio.NewScanner(decrypter)
	for scanner.Scan() {
		message := scanner.Text()
		logDebug(fmt.Sprintf("[%s]: %s", fromAddress, message))
		handleMessageReceived(message)
	}

	if !errors.Is(scanner.Err(), io.EOF) {
		logError(fmt.Sprintf("[%s] Connection closed with error: %s", fromAddress, scanner.Err().Error()))
	}
}
