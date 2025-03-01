//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"go.mukunda.com/oncache/oncrypt"
)

// How many outgoing messages each peer can buffer. If this is exceeded (usually due to
// a peer being unreachable for an extended period), then messages are dropped until it
// is processed.
var PeerQueueSize = 100

type peerSendQueue chan string

var DialerTimeout = 10 * time.Second
var peerLock sync.Mutex
var networkPeers = make(map[string]peerSendQueue)
var outgoingMessageQueue = make(chan string, 1000)
var incomingMessageQueue = make(chan string, 1000)

func registerPeer(wg *sync.WaitGroup, address string) {
	peerLock.Lock()
	defer peerLock.Unlock()

	if _, ok := networkPeers[address]; ok {
		return
	}

	queue := make(chan string, PeerQueueSize)

	networkPeers[address] = queue

	wg.Add(1)
	go peerDeliveryProcess(wg, address, queue)
}

func removePeer(address string) {
	peerLock.Lock()
	defer peerLock.Unlock()

	if _, ok := networkPeers[address]; ok {
		delete(networkPeers, address)
	}
}

func DispatchMessage(channel string, message string) {
	outgoingMessageQueue <- channel + " " + message
}

func handleChannel1Message(message string) {
	fields := strings.Fields(message)
	if len(fields) < 1 {
		logError("Invalid message on channel 1")
		return
	}
	cmd, rest, _ := strings.Cut(message, " ")
	rest = strings.TrimSpace(rest)
	if cmd == "DEL" {
		// datastring is a key
		invalidateLocal(rest)
	}
}

func handleMessageReceived(message string) {
	var channel, rest string
	channel, rest, _ = strings.Cut(message, " ")
	rest = strings.TrimSpace(rest)

	if channel == "1" {
		handleChannel1Message(rest)
	}

	// Check if channel has a callback and call that.
}

func messageSendProcess(wg *sync.WaitGroup) {
	defer catchProcessPanic("messageSendProcess", wg, func() { messageSendProcess(wg) })

	for {
		select {
		case message := <-outgoingMessageQueue:
			broadcastMessage(message)
		case <-shutdownSignal.C:
			return
		}
	}
}

func broadcastMessage(message string) {
	peerLock.Lock()
	defer peerLock.Unlock()

	for address, peerQueue := range networkPeers {
		select {
		case peerQueue <- message:
		default:
			// Skip if the queue is full
			logError(fmt.Sprintf("[%s] Peer queue full, dropping message.", address))
		}
	}
}

func waitForMessagesToSend(sendQueue chan string) (string, bool) {
	var messageData string
	select {
	case messageData = <-sendQueue:
	case <-shutdownSignal.C:
		return "", false
	}

	messageData += "\n"

	for {
		select {
		case additionalData := <-sendQueue:
			messageData += additionalData + "\n"
		default:
			return messageData, true
		}
	}
}

func peerDeliveryProcess(wg *sync.WaitGroup, address string, sendQueue chan string) {
	var conn net.Conn
	var encrypter io.Writer

	defer catchProcessPanic("peerProcess", wg, func() { peerDeliveryProcess(wg, address, sendQueue) })
	defer conn.Close()

	dialer := net.Dialer{Timeout: DialerTimeout}
	backoff := backoffRetry{period: 1.0, limit: 120.0, rate: 2.0}

restart:
	messages, ok := waitForMessagesToSend(sendQueue)
	if !ok {
		return
	}

	// Repeat this block until we establish a connection. If we can't get one in a timely
	// manner, then this peer will be dropped.
	startTime := time.Now().UnixMilli()
	for conn == nil {
		var err error
		conn, err = dialer.Dial("tcp", address)
		if err != nil {

			if time.Now().UnixMilli()-startTime > DialerTimeout.Milliseconds() {
				logError(fmt.Sprintf("[%s] Dial timeout; removing peer", address))
				removePeer(address)
				return
			}

			delay := int(backoff.get())
			logError(fmt.Sprintf("[%s] Error dialing %s; retrying in %d secs", address, err, delay))
			select {
			case <-time.After(time.Duration(delay) * time.Second):
			case <-shutdownSignal.C:
				return
			}
			continue
		}

		backoff.reset(1.0)

		// Connection established.
		encrypter, err = oncrypt.EncryptStream(encryptionKey, conn)
		if err != nil {
			logError(fmt.Sprintf("[%s] Error starting encrypted stream: %v; dropping messages and restarting", address, err))
			conn.Close()
			conn = nil
			goto restart
		}
	}

	_, err := encrypter.Write([]byte(messages))
	if err != nil {
		logError(fmt.Sprintf("[%s] Failed writing messages: %v; restarting", address, err))
		conn.Close()
		conn = nil
		encrypter = nil
	}
	goto restart
}
