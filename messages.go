//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import "strings"

type peerInfo struct {
	// Time of last successful send or receive.
	successTime int64
}

var peerMap = make(map[string]peerInfo)
var messageQueue = make(chan string, 1000)

func DispatchMessage(message string) {
	messageQueue <- message
}

func handleMessage(message string) {
	cmd, datastring, _ := strings.Cut(message, " ")
	if cmd == "DEL" {
		// datastring is a key
		Invalidate(datastring)
	}
}

func broadcastMessage(message string) {

}
