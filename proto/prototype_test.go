package proto

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestSend(t *testing.T) {
	listener, err := net.Listen("tcp", ":7750")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	recv := make(chan string, 4)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		go handleConnection(conn, recv)
	}()

	conn, err := net.Dial("tcp", "localhost:7750")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Fprintln(conn, "Hello, world!")
	fmt.Fprintln(conn, "Goodbye, world!")
	conn.Close()

	if <-recv != "Hello, world!" {
		t.Fail()
	}

	if <-recv != "Goodbye, world!" {
		t.Fail()
	}

	if len(recv) != 0 {
		t.Fail()
	}

}

func handleConnection(conn net.Conn, recv chan string) {
	defer conn.Close()
	time.Sleep(5 * time.Second)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		message := scanner.Text()
		fmt.Println("Received:", message)
		recv <- message
		//handleMessage(message)
	}
}
