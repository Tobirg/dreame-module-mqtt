// Minimal MQTT 3.1.1 client: QoS 0 publish/subscribe, last will, keepalive.
// Enough for a LAN broker (Mosquitto, the Zigbee2MQTT one, the Home Assistant add-on).
// shortcut: no TLS and no QoS 1/2; add them if a broker outside the LAN is needed.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type mqttOptions struct {
	Addr, ClientID, Username, Password string
	WillTopic, WillPayload             string
	KeepAlive                          time.Duration
}

type message struct{ Topic, Payload string }

type mqttClient struct {
	conn     net.Conn
	r        *bufio.Reader
	wmu      sync.Mutex
	Messages chan message
	done     chan struct{}
	err      error
}

func appendString(b []byte, s string) []byte {
	return append(append(b, byte(len(s)>>8), byte(len(s))), s...)
}

func packet(kind byte, body []byte) []byte {
	out := []byte{kind}
	n := len(body)
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		out = append(out, d)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

func readPacket(r *bufio.Reader) (byte, []byte, error) {
	kind, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, mul := 0, 1
	for i := 0; ; i++ {
		d, err := r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		n += int(d&0x7f) * mul
		mul *= 128
		if d&0x80 == 0 {
			break
		}
		if i == 3 {
			return 0, nil, errors.New("mqtt: malformed length")
		}
	}
	if n > 1<<20 {
		return 0, nil, errors.New("mqtt: packet too large")
	}
	body := make([]byte, n)
	_, err = io.ReadFull(r, body)
	return kind, body, err
}

func dialMQTT(o mqttOptions) (*mqttClient, error) {
	conn, err := net.DialTimeout("tcp", o.Addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	flags := byte(0x02) // clean session
	if o.WillTopic != "" {
		flags |= 0x04 | 0x20 // will, retained
	}
	if o.Username != "" {
		flags |= 0x80
		if o.Password != "" {
			flags |= 0x40
		}
	}
	body := appendString(nil, "MQTT")
	body = append(body, 4, flags, byte(o.KeepAlive/time.Second>>8), byte(o.KeepAlive/time.Second))
	body = appendString(body, o.ClientID)
	if o.WillTopic != "" {
		body = appendString(appendString(body, o.WillTopic), o.WillPayload)
	}
	if o.Username != "" {
		body = appendString(body, o.Username)
		if o.Password != "" {
			body = appendString(body, o.Password)
		}
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := &mqttClient{conn: conn, r: bufio.NewReader(conn), Messages: make(chan message, 16), done: make(chan struct{})}
	if _, err := conn.Write(packet(0x10, body)); err != nil {
		conn.Close()
		return nil, err
	}
	kind, ack, err := readPacket(c.r)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if kind != 0x20 || len(ack) != 2 {
		conn.Close()
		return nil, errors.New("mqtt: unexpected reply to CONNECT")
	}
	if ack[1] != 0 {
		conn.Close()
		reasons := map[byte]string{1: "protocol version refused", 2: "client id refused", 3: "server unavailable", 4: "bad user name or password", 5: "not authorized"}
		return nil, fmt.Errorf("mqtt: broker refused connection: %s", reasons[ack[1]])
	}
	_ = conn.SetDeadline(time.Time{})
	go c.readLoop(o.KeepAlive)
	go c.pingLoop(o.KeepAlive)
	return c, nil
}

func (c *mqttClient) readLoop(keepAlive time.Duration) {
	defer close(c.done)
	defer close(c.Messages)
	for {
		// The broker answers our pings, so silence longer than 1.5 keepalives means a dead link.
		_ = c.conn.SetReadDeadline(time.Now().Add(keepAlive * 3 / 2))
		kind, body, err := readPacket(c.r)
		if err != nil {
			c.err = err
			return
		}
		if kind&0xf0 != 0x30 || len(body) < 2 {
			continue // SUBACK, PINGRESP and the like
		}
		n := int(body[0])<<8 | int(body[1])
		if 2+n > len(body) {
			continue
		}
		rest := body[2+n:]
		if kind&0x06 != 0 { // QoS 1/2 carry a packet id; we subscribe with QoS 0 so this is defensive
			if len(rest) < 2 {
				continue
			}
			rest = rest[2:]
		}
		select {
		case c.Messages <- message{Topic: string(body[2 : 2+n]), Payload: string(rest)}:
		default: // a slow consumer drops commands rather than blocking pings
		}
	}
}

func (c *mqttClient) pingLoop(keepAlive time.Duration) {
	t := time.NewTicker(keepAlive / 2)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			if c.write(packet(0xc0, nil)) != nil {
				return
			}
		}
	}
}

func (c *mqttClient) write(p []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.conn.Write(p)
	return err
}

func (c *mqttClient) Publish(topic, payload string, retain bool) error {
	kind := byte(0x30)
	if retain {
		kind |= 0x01
	}
	return c.write(packet(kind, append(appendString(nil, topic), payload...)))
}

func (c *mqttClient) Subscribe(topics ...string) error {
	body := []byte{0, 1}
	for _, t := range topics {
		body = append(appendString(body, t), 0)
	}
	return c.write(packet(0x82, body))
}

// Done is closed when the connection is lost.
func (c *mqttClient) Done() <-chan struct{} { return c.done }

func (c *mqttClient) Close() {
	_ = c.write(packet(0xe0, nil)) // DISCONNECT: the broker drops the will
	c.conn.Close()
	<-c.done
}
