package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBroker accepts MQTT clients, records retained publishes and can push messages to them.
type fakeBroker struct {
	ln       net.Listener
	mu       sync.Mutex
	retained map[string]string
	connects []string // "user:password will=topic"
	conns    []net.Conn
	subs     []string
}

func newBroker(t *testing.T) *fakeBroker {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBroker{ln: ln, retained: map[string]string{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return b
}

func str(b []byte) (string, []byte) {
	n := int(b[0])<<8 | int(b[1])
	return string(b[2 : 2+n]), b[2+n:]
}

func (b *fakeBroker) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		kind, body, err := readPacket(r)
		if err != nil {
			return
		}
		switch kind & 0xf0 {
		case 0x10:
			_, rest := str(body)
			flags := rest[1]
			_, rest = str(rest[4:]) // client id
			info := ""
			if flags&0x04 != 0 {
				var topic string
				topic, rest = str(rest)
				_, rest = str(rest)
				info = " will=" + topic
			}
			var user, pass string
			if flags&0x80 != 0 {
				user, rest = str(rest)
			}
			if flags&0x40 != 0 {
				pass, _ = str(rest)
			}
			b.mu.Lock()
			b.connects = append(b.connects, user+":"+pass+info)
			b.conns = append(b.conns, c)
			b.mu.Unlock()
			code := byte(0)
			if user == "bad" {
				code = 4
			}
			c.Write([]byte{0x20, 2, 0, code})
		case 0x30:
			topic, payload := str(body)
			if kind&1 == 1 {
				b.mu.Lock()
				b.retained[topic] = string(payload)
				b.mu.Unlock()
			}
		case 0x80:
			rest := body[2:]
			for len(rest) > 0 {
				var topic string
				topic, rest = str(rest)
				rest = rest[1:]
				b.mu.Lock()
				b.subs = append(b.subs, topic)
				b.mu.Unlock()
			}
			c.Write([]byte{0x90, 3, body[0], body[1], 0})
		case 0xc0:
			c.Write([]byte{0xd0, 0})
		case 0xe0:
			return
		}
	}
}

func (b *fakeBroker) get(topic string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.retained[topic]
	return v, ok
}

func (b *fakeBroker) send(topic, payload string) {
	b.mu.Lock()
	c := b.conns[len(b.conns)-1]
	b.mu.Unlock()
	c.Write(packet(0x30, append(appendString(nil, topic), payload...)))
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatal("timed out waiting for", what)
}

// fakeAgent serves capabilities, an event stream fed from a channel, and records commands.
type fakeAgent struct {
	*httptest.Server
	events   chan string
	mu       sync.Mutex
	commands []string
}

func newAgent(t *testing.T) *fakeAgent {
	a := &fakeAgent{events: make(chan string, 4)}
	a.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer mod-token" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/capabilities":
			fmt.Fprint(w, `{"result":{"identity":{"model":"dreame.vacuum.r2505","did":"123","fw_ver":"4.3.9_2021"},"commands":{"empty":{"available":true},"wash_mop":{"available":true},"dry_start":{"available":false}}}}`)
		case "/api/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			w.(http.Flusher).Flush()
			for {
				select {
				case <-r.Context().Done():
					return
				case s := <-a.events:
					fmt.Fprintf(w, "event: state\ndata: %s\n\n", s)
					w.(http.Flusher).Flush()
				}
			}
		case "/api/v1/settings":
			fmt.Fprint(w, `{"result":[{"id":"child_lock","label":"Блокировка от детей","kind":"bool","value":0},`+
				`{"id":"cleaning_mode","label":"Режим уборки","kind":"select","options":[{"value":0,"label":"Сухая уборка"},{"value":2,"label":"Сухая и Влажная Уборка"}],"value":2},`+
				`{"id":"mop_pad_swing","label":"Покачивание швабры","kind":"select","auto":"MopScalable","options":[{"value":0,"label":"Выключено"},{"value":1,"label":"Авто"}],"value":-1},`+
				`{"id":"drying_time","label":"Время сушки","kind":"number","min":2,"max":12,"step":1,"value":3}]}`)
		case "/api/v1/map":
			fmt.Fprint(w, `{"result":{"map_id":2,"calibration_points":[{"vacuum":{"x":0,"y":0},"map":{"x":6,"y":-3}}],"rooms":{"1":{"name":"Кухня","x":0,"y":0}}}}`)
		case "/api/v1/map.png":
			w.Header().Set("Content-Type", "image/png")
			fmt.Fprint(w, "\x89PNG-test")
		case "/api/v1/command":
			var v map[string]any
			_ = json.NewDecoder(r.Body).Decode(&v)
			b, _ := json.Marshal(v)
			a.mu.Lock()
			a.commands = append(a.commands, string(b))
			a.mu.Unlock()
			fmt.Fprint(w, `{"result":{"code":0}}`)
		}
	}))
	t.Cleanup(a.Close)
	return a
}

func (a *fakeAgent) last() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.commands) == 0 {
		return ""
	}
	return a.commands[len(a.commands)-1]
}

func TestBridge(t *testing.T) {
	broker, agent := newBroker(t), newAgent(t)
	e := env{data: t.TempDir(), api: agent.URL, token: "mod-token", http: &http.Client{}}
	port := broker.ln.Addr().(*net.TCPAddr).Port
	configure := func(js string) {
		t.Helper()
		if _, err := e.action("configure", strings.NewReader(js)); err != nil {
			t.Fatal(err)
		}
	}
	configure(fmt.Sprintf(`{"enabled":true,"host":"127.0.0.1","port":%d,"username":"ha","password":"secret"}`, port))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.run(ctx, "test"); close(done) }()

	waitFor(t, "discovery", func() bool { _, ok := broker.get("homeassistant/vacuum/dreame_123/config"); return ok })
	waitFor(t, "map picture", func() bool { v, _ := broker.get("dreame_local/123/map"); return v == "\x89PNG-test" })
	if v, _ := broker.get("homeassistant/image/dreame_123/map/config"); !strings.Contains(v, `"image_topic":"dreame_local/123/map"`) {
		t.Fatal("map image entity", v)
	}
	if v, _ := broker.get("homeassistant/image/dreame_123/map/config"); !strings.Contains(v, `"json_attributes_topic":"dreame_local/123/map_attributes"`) {
		t.Fatal("map attributes topic", v)
	}
	if v, _ := broker.get("homeassistant/camera/dreame_123/map/config"); !strings.Contains(v, `"topic":"dreame_local/123/map"`) {
		t.Fatal("map camera", v)
	}
	if v, _ := broker.get("dreame_local/123/map_attributes"); !strings.Contains(v, `"calibration_points":[{"map":{"x":6,"y":-3},"vacuum":{"x":0,"y":0}}]`) {
		t.Fatal("map attributes", v)
	}
	waitFor(t, "settings", func() bool { _, ok := broker.get("dreame_local/123/settings"); return ok })
	if v, _ := broker.get("dreame_local/123/settings"); v != `{"child_lock":"OFF","cleaning_mode":"Сухая и Влажная Уборка","drying_time":3,"mop_pad_swing":"Выключено"}` {
		t.Fatal("settings state", v)
	}
	if v, _ := broker.get("homeassistant/switch/dreame_123/setting_child_lock/config"); !strings.Contains(v, `"command_topic":"dreame_local/123/setting/child_lock/set"`) || !strings.Contains(v, `"entity_category":"config"`) {
		t.Fatal("switch", v)
	}
	if v, _ := broker.get("homeassistant/select/dreame_123/setting_cleaning_mode/config"); !strings.Contains(v, `"options":["Сухая уборка","Сухая и Влажная Уборка"]`) {
		t.Fatal("select", v)
	}
	if v, _ := broker.get("homeassistant/number/dreame_123/setting_drying_time/config"); !strings.Contains(v, `"max":12`) {
		t.Fatal("number", v)
	}
	if broker.connects[0] != "ha:secret will=dreame_local/123/availability" {
		t.Fatal(broker.connects)
	}
	vac, _ := broker.get("homeassistant/vacuum/dreame_123/config")
	var cfg map[string]any
	_ = json.Unmarshal([]byte(vac), &cfg)
	if cfg["command_topic"] != "dreame_local/123/command" || cfg["device"].(map[string]any)["model"] != "dreame.vacuum.r2505" {
		t.Fatal(vac)
	}
	if v, _ := broker.get("homeassistant/button/dreame_123/empty/config"); !strings.Contains(v, `"payload_press":"empty"`) {
		t.Fatal("station button missing", v)
	}
	if v, ok := broker.get("homeassistant/button/dreame_123/dry_start/config"); !ok || v != "" {
		t.Fatal("unavailable command must clear its button", v, ok)
	}
	if st := e.status()["connection"].(map[string]any); st["state"] != "connected" {
		t.Fatal(st)
	}

	agent.events <- `{"stale":false,"state":{"activity":"docked","battery":95,"suction":1,"error":0,"error_name":"no_error","status_name":"charging","consumables":{"main_brush":{"left":52,"hours":158}}}}`
	waitFor(t, "state", func() bool {
		v, _ := broker.get("dreame_local/123/state")
		return strings.Contains(v, `"fan_speed":"Standard"`)
	})
	if v, _ := broker.get("dreame_local/123/state"); !strings.Contains(v, `"state":"docked"`) || !strings.Contains(v, `"battery":95`) {
		t.Fatal(v)
	}
	if v, _ := broker.get("dreame_local/123/state"); !strings.Contains(v, `"segments":{"1":"Кухня"}`) {
		t.Fatal("segments", v)
	}
	if !strings.Contains(vac, `"clean_segments_command_topic":"dreame_local/123/clean_segments"`) {
		t.Fatal(vac)
	}
	if v, _ := broker.get("dreame_local/123/state"); !strings.Contains(v, `"consumables":{"main_brush":52}`) {
		t.Fatal(v)
	}
	if v, _ := broker.get("homeassistant/sensor/dreame_123/main_brush/config"); !strings.Contains(v, `"value_template":"{{ value_json.consumables.main_brush }}"`) || !strings.Contains(v, `"name":"Main brush"`) {
		t.Fatal("consumable sensor", v)
	}
	if v, _ := broker.get("homeassistant/button/dreame_123/reset_main_brush/config"); !strings.Contains(v, `"payload_press":"reset_main_brush"`) || !strings.Contains(v, `"entity_category":"config"`) {
		t.Fatal("reset button", v)
	}
	waitFor(t, "online", func() bool { v, _ := broker.get("dreame_local/123/availability"); return v == "online" })
	agent.events <- `{"stale":true,"state":{"activity":"docked"}}`
	waitFor(t, "offline when stale", func() bool { v, _ := broker.get("dreame_local/123/availability"); return v == "offline" })

	for topic, want := range map[string]string{
		"dreame_local/123/command\x00return_to_base":                                                    `{"name":"dock"}`,
		"dreame_local/123/command\x00wash_mop":                                                          `{"name":"wash_mop"}`,
		"dreame_local/123/command\x00reset_main_brush":                                                  `{"consumable":"main_brush","name":"reset_consumable"}`,
		"dreame_local/123/fan_speed\x00turbo":                                                           `{"name":"set_suction","value":3}`,
		`dreame_local/123/send_command` + "\x00" + `{"command":"clean_rooms","params":{"rooms":[1,2]}}`: `{"name":"clean_rooms","rooms":[1,2]}`,
		// Home Assistant merges dict params into the top level.
		`dreame_local/123/send_command` + "\x00" + `{"command":"clean_rooms","rooms":[3]}`: `{"name":"clean_rooms","rooms":[3]}`,
	} {
		tp, payload, _ := strings.Cut(topic, "\x00")
		broker.send(tp, payload)
		waitFor(t, want, func() bool { return agent.last() == want })
	}

	// A settings change reconnects with the new credentials.
	configure(`{"enabled":true,"host":"127.0.0.1","port":` + fmt.Sprint(port) + `,"username":"ha","keep_password":true,"base_topic":"robot"}`)
	waitFor(t, "reconnect", func() bool { broker.mu.Lock(); defer broker.mu.Unlock(); return len(broker.connects) == 2 })
	if broker.connects[1] != "ha:secret will=robot/availability" {
		t.Fatal("password must be kept", broker.connects)
	}
	cancel()
	<-done
	if v, _ := broker.get("robot/availability"); v != "offline" {
		t.Fatal("stop must publish offline", v)
	}
	if e.status()["has_password"] != true || strings.Contains(fmt.Sprint(e.status()), "secret") {
		t.Fatal("status must not reveal the password")
	}
}

func TestBrokerRefusal(t *testing.T) {
	broker := newBroker(t)
	_, err := dialMQTT(mqttOptions{Addr: broker.ln.Addr().String(), ClientID: "x", Username: "bad", Password: "x", KeepAlive: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "bad user name or password") {
		t.Fatal(err)
	}
}

func TestValidate(t *testing.T) {
	for _, c := range []config{{Enabled: true}, {Host: "a b"}, {Host: "h", Port: 70000}, {Host: "h", BaseTopic: "a/#"}, {Host: "h:1883"}} {
		if validate(&c) == nil {
			t.Error("accepted", c)
		}
	}
	c := config{Host: " 192.168.2.5 ", BaseTopic: "/home/robot/"}
	if validate(&c) != nil || c.Host != "192.168.2.5" || c.Port != 1883 || c.BaseTopic != "home/robot" {
		t.Fatal(c)
	}
}
