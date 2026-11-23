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
	"sync/atomic"
	"testing"
	"time"
)

// fakeBroker accepts MQTT clients, records retained publishes and can push messages to them.
type fakeBroker struct {
	ln       net.Listener
	mu       sync.Mutex
	retained map[string]string
	live     map[string]string // the newest non-retained publish of each topic
	connects []string          // "user:password will=topic"
	conns    []net.Conn
	subs     []string
}

func newBroker(t *testing.T) *fakeBroker {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBroker{ln: ln, retained: map[string]string{}, live: map[string]string{}}
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
			b.mu.Lock()
			if kind&1 == 1 {
				b.retained[topic] = string(payload)
			} else {
				b.live[topic] = string(payload)
			}
			b.mu.Unlock()
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
	history  string      // GET /api/v1/history result
	light    bool        // camera fill light
	noCamera atomic.Bool // the camera module is not installed
}

func newAgent(t *testing.T) *fakeAgent {
	a := &fakeAgent{events: make(chan string, 4), history: `[{"start":"2026-11-22T10:00:00Z","result":"docked"}]`}
	a.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer mod-token" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/capabilities":
			fmt.Fprint(w, `{"result":{"identity":{"model":"dreame.vacuum.r2505","did":"123","fw_ver":"4.3.9_2021"},"commands":{"empty":{"available":true},"wash_mop":{"available":true},"dry_start":{"available":false},"remote_move":{"available":true}}}}`)
		case "/api/v1/m/camera/status", "/api/v1/m/camera/light":
			if a.noCamera.Load() {
				w.WriteHeader(404)
				fmt.Fprint(w, `{"error":"no such module"}`)
				return
			}
			var p struct{ On *bool }
			_ = json.NewDecoder(r.Body).Decode(&p)
			a.mu.Lock()
			if p.On != nil {
				a.light = *p.On
			}
			fmt.Fprintf(w, `{"result":{"enabled":true,"light":%v}}`, a.light)
			a.mu.Unlock()
		case "/api/v1/m/camera/live":
			fmt.Fprint(w, `{"result":{"at":1,"jpeg":"/9j/frame"}}`)
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
		case "/api/v1/history":
			a.mu.Lock()
			fmt.Fprintf(w, `{"result":%s}`, a.history)
			a.mu.Unlock()
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
	saved := mapEvery
	mapEvery = 200 * time.Millisecond // the history is read with the map
	defer func() { mapEvery = saved }()
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
	if v, _ := broker.get("homeassistant/button/dreame_123/turn_left/config"); !strings.Contains(v, `"payload_press":"turn_left"`) {
		t.Fatal("remote control button", v)
	}
	if v, _ := broker.get("homeassistant/camera/dreame_123/front/config"); !strings.Contains(v, `"image_encoding":"b64"`) {
		t.Fatal("camera entity", v)
	}
	if v, _ := broker.get("dreame_local/123/camera_live"); v != "OFF" {
		t.Fatal("camera stream starts off", v)
	}
	broker.send("dreame_local/123/camera_live/set", "ON")
	waitFor(t, "camera frame", func() bool {
		broker.mu.Lock()
		defer broker.mu.Unlock()
		return broker.live["dreame_local/123/camera"] == "/9j/frame"
	})
	if v, _ := broker.get("dreame_local/123/camera_live"); v != "ON" {
		t.Fatal("camera stream switch", v)
	}
	if _, ok := broker.get("dreame_local/123/camera"); ok {
		t.Fatal("camera frames must not be retained")
	}
	broker.send("dreame_local/123/camera_live/set", "OFF")
	waitFor(t, "camera off", func() bool { v, _ := broker.get("dreame_local/123/camera_live"); return v == "OFF" })
	if v, _ := broker.get("homeassistant/switch/dreame_123/camera_light/config"); !strings.Contains(v, `"command_topic":"dreame_local/123/camera_light/set"`) {
		t.Fatal("camera light switch", v)
	}
	if v, _ := broker.get("dreame_local/123/camera_light"); v != "OFF" {
		t.Fatal("camera light state", v)
	}
	broker.send("dreame_local/123/camera_light/set", "ON")
	waitFor(t, "light on", func() bool { v, _ := broker.get("dreame_local/123/camera_light"); return v == "ON" })
	// Removing the camera module stops a running stream and its entities.
	broker.send("dreame_local/123/camera_live/set", "ON")
	waitFor(t, "camera on", func() bool { v, _ := broker.get("dreame_local/123/camera_live"); return v == "ON" })
	agent.noCamera.Store(true)
	broker.send("dreame_local/123/setting/child_lock/set", "ON") // settings, and the camera, are read again 3 s later
	waitFor(t, "stream off without the module", func() bool { v, _ := broker.get("dreame_local/123/camera_live"); return v == "OFF" })
	if v, _ := broker.get("homeassistant/switch/dreame_123/camera_light/config"); v != "" {
		t.Fatal("light switch must go with the module", v)
	}
	if st := e.status()["connection"].(map[string]any); st["state"] != "connected" {
		t.Fatal(st)
	}

	agent.events <- `{"stale":false,"state":{"activity":"docked","battery":95,"suction":1,"error":0,"error_name":"no_error","status_name":"charging","consumables":{"main_brush":{"left":52,"hours":158}},` +
		`"volume":80,"charging":1,"station":2,"cleaned_area":12,"cleaning_time":20,"stats":{"area":323,"cleanings":37,"minutes":373},"dnd":{"enabled":false,"start":"22:00","end":"08:00"}}}`
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
	if v, _ := broker.get("dreame_local/123/state"); !strings.Contains(v, `"station_state":"drying"`) || !strings.Contains(v, `"charging":true`) ||
		!strings.Contains(v, `"dnd_start":"22:00:00"`) || !strings.Contains(v, `"consumable_hours":{"main_brush":158}`) || !strings.Contains(v, `"totals":{"area":323`) {
		t.Fatal("state for the new entities", v)
	}
	for topic, want := range map[string]string{
		"homeassistant/sensor/dreame_123/total_cleaned_area/config": `"value_template":"{{ value_json.totals.area }}"`,
		"homeassistant/binary_sensor/dreame_123/charging/config":    `"device_class":"battery_charging"`,
		"homeassistant/binary_sensor/dreame_123/problem/config":     `"device_class":"problem"`,
		"homeassistant/number/dreame_123/volume/config":             `"command_topic":"dreame_local/123/volume/set"`,
		"homeassistant/time/dreame_123/dnd_end/config":              `"command_topic":"dreame_local/123/dnd_end/set"`,
		"homeassistant/event/dreame_123/cleaning/config":            `"event_types":["finished","failed"]`,
	} {
		if v, _ := broker.get(topic); !strings.Contains(v, want) {
			t.Fatal(topic, v)
		}
	}
	// A run that finishes after connecting is an event; the one already in the history is not.
	agent.mu.Lock()
	agent.history = `[{"start":"2026-11-23T10:00:00Z","result":"error","error":"bin_full","minutes":5},{"start":"2026-11-22T10:00:00Z","result":"docked"}]`
	agent.mu.Unlock()
	waitFor(t, "cleaning event", func() bool {
		broker.mu.Lock()
		defer broker.mu.Unlock()
		return strings.Contains(broker.live["dreame_local/123/cleaning_event"], `"event_type":"failed"`)
	})
	waitFor(t, "online", func() bool { v, _ := broker.get("dreame_local/123/availability"); return v == "online" })
	agent.events <- `{"stale":true,"state":{"activity":"docked"}}`
	waitFor(t, "offline when stale", func() bool { v, _ := broker.get("dreame_local/123/availability"); return v == "offline" })

	for topic, want := range map[string]string{
		"dreame_local/123/command\x00return_to_base":                                                    `{"name":"dock"}`,
		"dreame_local/123/command\x00move_forward":                                                      `{"name":"remote_move","rotation":0,"velocity":200}`,
		"dreame_local/123/command\x00wash_mop":                                                          `{"name":"wash_mop"}`,
		"dreame_local/123/command\x00reset_main_brush":                                                  `{"consumable":"main_brush","name":"reset_consumable"}`,
		"dreame_local/123/volume/set\x0055":                                                             `{"name":"set_volume","value":55}`,
		"dreame_local/123/dnd_end/set\x0007:30:00":                                                      `{"enabled":false,"end":"07:30","name":"set_dnd","start":"22:00"}`,
		"dreame_local/123/fan_speed\x00turbo":                                                           `{"name":"set_suction","value":3}`,
		`dreame_local/123/send_command` + "\x00" + `{"command":"clean_rooms","params":{"rooms":[1,2]}}`: `{"name":"clean_rooms","repeats":1,"rooms":[1,2],"suction":1,"water":2}`,
		// Home Assistant merges dict params into the top level.
		`dreame_local/123/send_command` + "\x00" + `{"command":"clean_rooms","rooms":[3]}`: `{"name":"clean_rooms","repeats":1,"rooms":[3],"suction":1,"water":2}`,
		// The map card: string room ids, fractional coordinates, its own repeats.
		`dreame_local/123/send_command` + "\x00" + `{"command":"clean_rooms","rooms":["4"],"repeats":2}`:                        `{"name":"clean_rooms","repeats":2,"rooms":[4],"suction":1,"water":2}`,
		`dreame_local/123/send_command` + "\x00" + `{"command":"clean_zone","zones":[[-5000.4,"1000",-3800,2200]],"repeats":1}`: `{"name":"clean_zone","repeats":1,"suction":1,"water":2,"zones":[[-5000,1000,-3800,2200]]}`,
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

func TestCardYAML(t *testing.T) {
	card := map[string]any{"rooms": map[string]any{
		"10": map[string]any{"name": "Кухня", "x": 1, "y": 2, "outline": [][2]int{{0, 0}, {100, 0}, {100, 100}}},
		"4":  map[string]any{"name": `Комната "4"`, "x": -5, "y": 6, "outline": [][2]int{}},
	}}
	y := cardYAML("dreame.vacuum.r2505", card)
	for _, want := range []string{"entity: vacuum.dreame_r2505\n", "camera: camera.dreame_r2505_map_camera\n", "command: clean_zone",
		`params: {points: [["[[point_x]]", "[[point_y]]"]], repeats: "[[repeats]]"}`,
		"      - id: \"4\"\n        label: {text: \"Комната \\\"4\\\"\", x: -5, 'y': 6}\n        outline: []\n      - id: \"10\"",
		"outline: [[0,0],[100,0],[100,100]]"} {
		if !strings.Contains(y, want) {
			t.Fatalf("missing %q in\n%s", want, y)
		}
	}
	if strings.Contains(cardYAML("dreame.vacuum.r2505", nil), "ROOM") {
		t.Fatal("no rooms without a map")
	}
}

func TestVacuumAttributes(t *testing.T) {
	state := map[string]any{"status_name": "charging", "battery": 95.0, "cleaned_area": 2.0, "stats": map[string]any{"cleanings": 33.0, "area": 310.0, "minutes": 360.0}}
	settings := []setting{{ID: "cleaning_mode", Value: 2.0}, {ID: "mop_humidity", Value: 3.0}, {ID: "tight_mopping", Value: 0.0},
		{ID: "wetness_level", Value: 12.0}, {ID: "cleaning_route", Value: 9.0}, {ID: "child_lock", Value: true}}
	want := `{"battery_level":95,"cleaned_area":2,"cleaning_count":33,"cleaning_mode":"sweeping and mopping","mop_pad_humidity":"wet",` +
		`"status":"charging","tight_mopping":"false","total_cleaned_area":310,"total_cleaning_time":360,"wetness_level":12}`
	if got := vacuumAttributes(state, settings); got != want {
		t.Fatal(got)
	}
}

func TestNoCameraWithoutCameraModule(t *testing.T) {
	d := discovery(topicsFor(config{}, "1"), capabilities{}, "x", nil, nil, false, true)
	for _, topic := range []string{"homeassistant/camera/dreame_1/front/config", "homeassistant/switch/dreame_1/camera_live/config", "homeassistant/switch/dreame_1/camera_light/config", "homeassistant/button/dreame_1/move_forward/config"} {
		if v, ok := d[topic]; !ok || v != "" {
			t.Errorf("%s must be cleared, got %q", topic, v)
		}
	}
}

func TestSettingNamesAndHidden(t *testing.T) {
	common := func(m map[string]any) string { b, _ := json.Marshal(m); return string(b) }
	d := settingDiscovery(topicsFor(config{}, "1"), common, []setting{{ID: "lds_state", Label: "LDS", Kind: "bool"}, {ID: "fill_light", Label: "Подсветка камеры", Kind: "bool"}})
	if v := d["homeassistant/switch/dreame_1/setting_lds_state/config"]; !strings.Contains(v, `"enabled_by_default":false`) {
		t.Fatal("service setting must start disabled", v)
	}
	if v := d["homeassistant/switch/dreame_1/setting_fill_light/config"]; !strings.Contains(v, `"name":"Подсветка в темноте (для ИИ)"`) || strings.Contains(v, "enabled_by_default") {
		t.Fatal("fill light", v)
	}
}
