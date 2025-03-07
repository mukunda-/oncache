//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import (
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"time"

	"go.mukunda.com/oncache/oncrypt"
)

// How many outgoing messages each peer can buffer. If this is exceeded (usually due to
// a peer being unreachable for an extended period), then messages are dropped until it
// is processed.
var PeerQueueSize = 100

type peerSendQueue chan string

type networkPeer struct {
	address string
	queue   peerSendQueue
	cancel  signal
}

var DialerTimeout = 10 * time.Second

func (oc *Oncache) registerPeer(address string) {
	oc.peerLock.Lock()
	defer oc.peerLock.Unlock()

	if _, ok := oc.networkPeers[address]; ok {
		return
	}

	queue := make(chan string, PeerQueueSize)
	oc.networkPeers[address] = &networkPeer{address, queue, newSignal()}

	oc.activeWork.Add(1)
	go oc.peerDeliveryProcess(address, queue)
}

func (oc *Oncache) removePeer(address string) {
	oc.peerLock.Lock()
	defer oc.peerLock.Unlock()

	if peer, ok := oc.networkPeers[address]; ok {
		peer.cancel.raise()
		delete(oc.networkPeers, address)
	}
}

// Send a message to all peers. `channel` is a string that identifies the message type.
// Channel "1" is used by the Oncache system (invalidations, etc). Channel strings should
// be lowercase alphanumeric with no spaces.
//
// Messages cannot contain line breaks. LF is the stream delimiter.
func (oc *Oncache) DispatchMessage(channel string, message string) {
	if channel == "" {
		logError("Attempted to send message on empty channel " + string(debug.Stack()))
		return
	}

	if strings.Contains(channel, "\n") || strings.Contains(message, "\n") {
		logError("Attempted to send message with line break.")
		return
	}

	logDebug("Dispatching message; chan=" + channel + " msg=" + message)
	oc.outgoingMessageQueue <- channel + " " + message
}

// Handle a message on channel "1" (system).
func (oc *Oncache) handleChannel1Message(message string) {
	fields := strings.Fields(message)
	if len(fields) < 1 {
		logError("Received invalid message on channel 1 (too short).")
		return
	}

	cmd, rest, _ := strings.Cut(message, " ")
	rest = strings.TrimSpace(rest)
	if cmd == "DEL" {
		// datastring is a key
		if ManagerLock != nil {
			ManagerLock.Lock()
			defer ManagerLock.Unlock()
		}
		oc.deleteLocal(rest)
	} else {
		logError(fmt.Sprintf("Received unknown command on channel 1: %s", message))
	}
}

// Called when a message is received from a peer.
func (oc *Oncache) handleMessageReceived(host string, fullMessage string) {
	var channel, rest string
	channel, rest, _ = strings.Cut(fullMessage, " ")
	rest = strings.TrimSpace(rest)

	if channel == "1" {
		oc.handleChannel1Message(rest)
	}

	oc.subscriptionLock.Lock()
	defer oc.subscriptionLock.Unlock()

	channelSubs := oc.subscriptions[channel]
	for _, sub := range channelSubs {
		sub.handler(host, channel, rest)
	}

	fullSubs := oc.subscriptions[""]
	for _, sub := range fullSubs {
		sub.handler(host, channel, rest)
	}
}

// Sending work process. This monitors the queue and broadcasts to peer channels.
func (oc *Oncache) messageSendProcess() {
	defer oc.onProcessCompleted(
		"messageSendProcess",
		func() { oc.messageSendProcess() },
	)

	for {
		select {
		case message := <-oc.outgoingMessageQueue:
			oc.broadcastMessage(message)
		case <-oc.stopSignal.C:
			return
		}
	}
}

// Submit a message to all peer queues.
func (oc *Oncache) broadcastMessage(message string) {
	oc.peerLock.RLock()
	defer oc.peerLock.RUnlock()

	for address, peer := range oc.networkPeers {
		select {
		case peer.queue <- message:
		default:
			// If the queue is full, drop the message. This may happen if a peer becomes
			// unresponsive. It will either recover or be dropped from the system later.
			logError(fmt.Sprintf("[%s] Peer queue full, dropping message.", address))
		}
	}
}

// Monitor the outgoing queue for a peer for new messages. When they are submitted, gather
// all of them and return as a single LF-delimited string.
func (oc *Oncache) waitForMessagesToSend(sendQueue chan string) (string, bool) {
	var messageData string
	select {
	case messageData = <-sendQueue:
		// New message received.
	case <-oc.stopSignal.C:
		// System is shutting down. Escape.
		return "", false
	}

	messageData += "\n"

	// Gather any additional messages in the queue.
	for {
		select {
		case additionalData := <-sendQueue:
			messageData += additionalData + "\n"
		default:
			// No more messages, return.
			return messageData, true
		}
	}
}

// Process for connecting to a peer and sending messages. This process is started per
// peer.
func (oc *Oncache) peerDeliveryProcess(address string,
	sendQueue chan string) {

	var conn net.Conn
	var encrypter io.Writer

	defer oc.onProcessCompleted("peerProcess",
		func() { oc.peerDeliveryProcess(address, sendQueue) },
	)
	defer func() {
		if conn != nil {
			conn.Close() // Make sure the channel is closed if we exit this function.
		}
	}()

	dialer := net.Dialer{Timeout: DialerTimeout}
	backoff := backoffRetry{period: 1.0, limit: 120.0, rate: 2.0}

restart:
	messages, ok := oc.waitForMessagesToSend(sendQueue)
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
				logError(fmt.Sprintf("[%s] Dial timeout; removing peer.", address))
				oc.removePeer(address)
				return
			}

			delay := int(backoff.get())
			logError(fmt.Sprintf("[%s] Error dialing %s; retrying in %d secs.", address, err, delay))
			select {
			case <-time.After(time.Duration(delay) * time.Second):
			case <-oc.stopSignal.C:
				return
			case <-oc.networkPeers[address].cancel.C:
				return
			}
			continue
		}

		backoff.reset(1.0)

		// Connection established.
		encrypter, err = oncrypt.EncryptStream(oc.encryptionKey, conn)
		if err != nil {
			logError(fmt.Sprintf("[%s] Error starting encrypted stream: %v; dropping messages and restarting", address, err))
			conn.Close()
			conn = nil
			goto restart
		}

		_, err = encrypter.Write([]byte("v1 " + oc.hostWithPort() + "\n"))
		if err != nil {
			logError(fmt.Sprintf("[%s] Failed writing connection hello: %v; restarting", address, err))
			conn.Close()
			conn = nil
			encrypter = nil
			goto restart
		}
	}

	// DEBUG: remove these logs
	logDebug(fmt.Sprintf("writing messages to %s: %s", conn.RemoteAddr().String(), messages))
	_, err := encrypter.Write([]byte(messages))
	logDebug(fmt.Sprintf("writing messages to %s done", conn.RemoteAddr().String()))
	if err != nil {
		logError(fmt.Sprintf("[%s] Failed writing messages: %v; restarting.", address, err))
		conn.Close()
		conn = nil
		encrypter = nil
	}
	goto restart
}

func (oc *Oncache) Subscribe(channel string, handler MessageHandler) MessageSubscriberId {
	oc.subscriptionLock.Lock()
	defer oc.subscriptionLock.Unlock()

	if _, ok := oc.subscriptions[channel]; !ok {
		oc.subscriptions[channel] = make([]MessageSubscriber, 0)
	}

	id := oc.nextSubscriptionId
	oc.nextSubscriptionId++

	oc.subscriptions[channel] = append(oc.subscriptions[channel], MessageSubscriber{
		id:      id,
		handler: handler,
	})

	return id
}
