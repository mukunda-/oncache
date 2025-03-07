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

// How many outgoing messages each peer can buffer. If this is exceeded (usually due to
// a peer being unreachable for an extended period), then messages are dropped until it
// is processed.
var PeerQueueSize = 100

// If the dialer times out, then it will be retried later. If it repeatedly times out,
// then the node is removed from the pool.
var DialerTimeout = 10 * time.Second

// How long to delay before retrying to set up a listener after failing to start.
var ListenerFailedStartRetryTime = time.Minute

// Test diagnostic flags.

// This will cause the listener process to panic during the startup phase.
var debugCrashListener = false

type peerSendQueue chan string

type networkPeer struct {
	host   string
	queue  peerSendQueue
	cancel signal
}

func (oc *Oncache) registerPeer(host string, fromSub bool) {
	oc.peerLock.Lock()
	defer oc.peerLock.Unlock()

	if _, ok := oc.networkPeers[host]; ok {
		logDebug(fmt.Sprintf("Peer %s is already registered.", host))
		return
	}

	queue := make(chan string, PeerQueueSize)
	if !fromSub {
		// We don't request SUB if this registration was from a SUB. The peer should already
		// be aware of us (as it connected to us).
		queue <- "1 SUB"
	}

	oc.networkPeers[host] = &networkPeer{host, queue, newSignal()}

	oc.activeWork.Add(1)
	go oc.peerDeliveryProcess(host, queue)
}

func (oc *Oncache) removePeer(host string) {
	oc.peerLock.Lock()
	defer oc.peerLock.Unlock()

	if peer, ok := oc.networkPeers[host]; ok {
		peer.cancel.raise()
		delete(oc.networkPeers, host)
	}
}

// Send a message to all peers. `channel` is a string that identifies the message type.
// Channel "1" is used by the Oncache system (invalidations, etc). Channel strings should
// be lowercase alphanumeric with no spaces.
//
// Messages cannot contain line breaks. LF is the stream delimiter.
func (oc *Oncache) DispatchMessage(channel string, message string) {
	if channel == "" {
		logError("Attempted to send message on empty channel; " + string(debug.Stack()))
		return
	}

	if strings.Contains(channel, "\n") || strings.Contains(message, "\n") {
		logError("Attempted to send message with line break;" + string(debug.Stack()))
		return
	}

	logDebug("Dispatching message; chan=" + channel + " msg=" + message)
	oc.outgoingMessageQueue <- channel + " " + message
}

// Handle a message on channel "1" (system).
func (oc *Oncache) handleChannel1Message(host string, message string) {
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
	} else if cmd == "SUB" {
		logDebug(fmt.Sprintf("Registering peer %s from SUB command.", host))
		oc.registerPeer(host, true)
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
		oc.handleChannel1Message(host, rest)
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

	for host, peer := range oc.networkPeers {
		select {
		case peer.queue <- message:
		default:
			// If the queue is full, drop the message. This may happen if a peer becomes
			// unresponsive. It will either recover or be dropped from the system later.
			logError(fmt.Sprintf("[%s] Peer queue full, dropping message.", host))
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

// A simple backoff-retry calculator.
type backoffRetry struct {
	period float64 // current backoff period
	limit  float64 // maximum backoff period
	rate   float64 // rate of increase per failure
}

// Reset the backoff period to the initial value (called after success).
func (br *backoffRetry) reset(period float64) {
	br.period = period
}

// Get the current backoff period and increment it (called per failure).
func (br *backoffRetry) get() float64 {
	period := br.period
	br.period = br.period * br.rate
	if br.period > br.limit {
		br.period = br.limit
	}
	return period
}

// Process for connecting to a peer and sending messages. This process is started per
// peer.
func (oc *Oncache) peerDeliveryProcess(host string,
	sendQueue chan string) {

	var conn net.Conn
	var encrypter io.Writer

	defer oc.onProcessCompleted("peerDeliveryProcess",
		func() { oc.peerDeliveryProcess(host, sendQueue) },
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
	dialTimeout := time.Now().Add(DialerTimeout)

	for conn == nil {
		var err error
		conn, err = dialer.Dial("tcp", host)
		if err != nil {

			if time.Now().After(dialTimeout) {
				logError(fmt.Sprintf("[%s] Dial timeout; removing peer.", host))
				oc.removePeer(host)
				return
			}

			delay := int(backoff.get())
			logError(fmt.Sprintf("[%s] Error dialing %s; retrying in %d secs.", host, err, delay))
			select {
			case <-time.After(time.Duration(delay) * time.Second):
			case <-oc.stopSignal.C:
				return
			case <-oc.networkPeers[host].cancel.C:
				return
			}
			continue
		}

		backoff.reset(1.0)

		// Connection established.
		encrypter, err = oncrypt.EncryptStream(oc.encryptionKey, conn)
		if err != nil {
			logError(fmt.Sprintf("[%s] Error starting encrypted stream: %v; dropping messages and restarting", host, err))
			conn.Close()
			conn = nil
			goto restart
		}

		_, err = encrypter.Write([]byte("v1 " + oc.hostWithPort() + "\n"))
		if err != nil {
			logError(fmt.Sprintf("[%s] Failed writing connection hello: %v; restarting", host, err))
			conn.Close()
			conn = nil
			encrypter = nil
			goto restart
		}
	}

	_, err := encrypter.Write([]byte(messages))
	if err != nil {
		logError(fmt.Sprintf("[%s] Failed writing messages: %v; restarting.", host, err))
		conn.Close()
		conn = nil
		encrypter = nil
	}
	goto restart
}

// This process listens for connections and spawns goroutines for handling clients. When
// a client connects, it registers them as a peer.
func (oc *Oncache) listenerProcess() {
	// If anything causes an unexpected panic, try to restart.
	defer oc.onProcessCompleted("listenerProcess", func() { oc.listenerProcess() })
	var listener net.Listener
	var err error
	defer func() {
		if listener != nil {
			listener.Close()
		}
	}()

restart:
	// Make sure we're in a good closed state after a restart.
	if listener != nil {
		listener.Close()
	}

	select {
	case <-oc.stopSignal.C:
		return
	default:
	}

	listener, err = net.Listen("tcp", fmt.Sprintf(":%d", oc.listenPort))
	if err != nil {
		logError(fmt.Sprintf("Failed to start listener: %s; retrying in %d seconds.",
			err,
			int(ListenerFailedStartRetryTime.Seconds())))

		select {
		case <-time.After(ListenerFailedStartRetryTime):
			// Okay, retry.
		case <-oc.stopSignal.C:
			// Shutdown raised during sleep, exit.
			return
		}

		goto restart
	}

	if debugCrashListener {
		panic("debugCrashListener")
	}

	logInfo(fmt.Sprintf("Listening on port %d", oc.listenPort))
	oc.initialListenerStarted.raise()
	listenerExited := make(chan struct{})

	go func() {
		defer close(listenerExited)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
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

// Subscribe to messages on a channel. The handler will be called when a message is
// received on the specified channel. If the channel is empty, the handler will be called
// for all messages on any channel.
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

// Remove a subscription. Pass the ID returned by Subscribe. The channel must also match
// what channel was subscribed to, or "" if it was a global subscription.
func (oc *Oncache) Unsubscribe(channel string, id MessageSubscriberId) {
	oc.subscriptionLock.Lock()
	defer oc.subscriptionLock.Unlock()

	subs := oc.subscriptions[channel]
	for i, sub := range subs {
		if sub.id == id {
			oc.subscriptions[channel] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

// Returns the number of peers we have registered in the network (excluding this node).
func (oc *Oncache) NumPeers() int {
	oc.peerLock.RLock()
	defer oc.peerLock.RUnlock()
	return len(oc.networkPeers)
}
