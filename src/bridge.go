package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type config struct {
	Enabled         bool   `json:"enabled"`
	Host            string `json:"host"`
	Port            int    `json:"port"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	BaseTopic       string `json:"base_topic"`       // default dreame_local/<did>
	DiscoveryPrefix string `json:"discovery_prefix"` // default homeassistant
}

type env struct {
	data, api, token string
	http             *http.Client
}

func (e env) configPath() string { return filepath.Join(e.data, "config.json") }

func (e env) loadConfig() (config, time.Time) {
	c := config{Port: 1883, DiscoveryPrefix: "homeassistant"}
	st, err := os.Stat(e.configPath())
	if err != nil {
		return c, time.Time{}
	}
	b, _ := os.ReadFile(e.configPath())
	_ = json.Unmarshal(b, &c)
	return c, st.ModTime()
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (e env) setStatus(state, broker, msg string) {
	_ = writeJSON(filepath.Join(e.data, "status.json"), map[string]any{"state": state, "broker": broker, "error": msg, "since": time.Now().UTC().Format(time.RFC3339)})
}

// ---- agent API

func (e env) get(ctx context.Context, path string, v any) error {
	return e.call(ctx, "GET", path, nil, v)
}

// post sends params (an empty object when nil) and reads the result into v, if not nil.
func (e env) post(ctx context.Context, path string, params, v any) error {
	if params == nil {
		params = map[string]any{}
	}
	return e.call(ctx, "POST", path, params, v)
}

func (e env) call(ctx context.Context, method, path string, params, v any) error {
	var body io.Reader
	if params != nil {
		b, _ := json.Marshal(params)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, e.api+path, body)
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := e.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&out); err != nil && res.StatusCode == 200 {
		return err
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("agent refused %s: %s", path, out.Error)
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(out.Result, v)
}

var mapEvery, settingsEvery = 15 * time.Second, 2 * time.Minute

// The camera stream: a frame every cameraEvery while the switch is on, off by itself after cameraFor.
var cameraEvery, cameraFor = 500 * time.Millisecond, 10 * time.Minute

// cameraLive publishes the camera module's live frames until ctx ends; a timeout turns the switch off.
func (e env) cameraLive(ctx context.Context, client *mqttClient, t topics) {
	defer func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			_ = client.Publish(t.base+"/camera_live", "OFF", true)
		}
	}()
	tick := time.NewTicker(cameraEvery)
	defer tick.Stop()
	last := ""
	for {
		var f struct {
			JPEG   string `json:"jpeg"`
			State  string `json:"state"`
			Reason string `json:"reason"`
		}
		status := map[string]string{"state": "streaming"}
		if err := e.post(ctx, "/api/v1/m/camera/live", nil, &f); err != nil {
			status = map[string]string{"state": "error", "reason": err.Error()}
		} else if f.JPEG != "" {
			_ = client.Publish(t.base+"/camera", f.JPEG, false) // not retained: no picture of the home stays on the broker
		} else {
			status = map[string]string{"state": f.State, "reason": f.Reason} // starting, or the robot sleeps
		}
		if b, _ := json.Marshal(status); string(b) != last && ctx.Err() == nil {
			last = string(b)
			_ = client.Publish(t.base+"/camera_status", last, true)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// settingTopic returns the setting id of <base>/setting/<id>/set.
func settingTopic(t topics, topic string) (string, bool) {
	id, ok := strings.CutPrefix(topic, t.base+"/setting/")
	if id, ok2 := strings.CutSuffix(id, "/set"); ok && ok2 && settingIDRe.MatchString(id) {
		return id, true
	}
	return "", false
}

// raw returns a non-JSON agent resource such as the map picture.
func (e env) raw(ctx context.Context, path string) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", e.api+path, nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	res, err := e.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("agent %s: HTTP %d", path, res.StatusCode)
	}
	return io.ReadAll(io.LimitReader(res.Body, 4<<20))
}

type snapshot struct {
	Stale bool           `json:"stale"`
	State map[string]any `json:"state"`
}

// events delivers agent snapshots until ctx ends; it reconnects on its own.
func (e env) events(ctx context.Context, out chan<- *snapshot) {
	for ctx.Err() == nil {
		req, _ := http.NewRequestWithContext(ctx, "GET", e.api+"/api/v1/events", nil)
		req.Header.Set("Authorization", "Bearer "+e.token)
		res, err := e.http.Do(req)
		if err == nil && res.StatusCode == 200 {
			lines := bufio.NewScanner(res.Body)
			lines.Buffer(make([]byte, 64<<10), 1<<20)
			for lines.Scan() {
				data, ok := strings.CutPrefix(lines.Text(), "data: ")
				var s snapshot
				if ok && json.Unmarshal([]byte(data), &s) == nil {
					select {
					case out <- &s:
					case <-ctx.Done():
					}
				}
			}
		}
		if res != nil {
			res.Body.Close()
		}
		select {
		case out <- nil: // stream lost: the robot is unavailable until it comes back
		case <-ctx.Done():
		}
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
}

// ---- Home Assistant mapping

var consumableRe = regexp.MustCompile(`^[a-z_]{1,40}$`)

// Names xiaomi-vacuum-map-card has icons and translations for (the app calls the first one Quiet).
var fanSpeeds = []string{"Silent", "Standard", "Strong", "Turbo"}

// Remote control (agent 0.25+): one press is one short move, about 14 cm or a turn step. The first
// one drives the robot off the dock; it ends remote control by itself about 15 s after the last.
var moves = []struct {
	command, name, icon string
	velocity, rotation  int
}{
	{"move_forward", "Move forward", "mdi:arrow-up-bold", 200, 0},
	{"move_backward", "Move backward", "mdi:arrow-down-bold", -150, 0},
	{"turn_left", "Turn left", "mdi:rotate-left", 0, 60},
	{"turn_right", "Turn right", "mdi:rotate-right", 0, -60},
}

// Sensors read from <base>/state; field is the state key.
var sensors = []struct {
	id, name, field string
	extra           map[string]any
}{
	{"cleaned_area", "Cleaned area", "cleaned_area", map[string]any{"device_class": "area", "unit_of_measurement": "m²", "state_class": "measurement"}},
	{"cleaning_time", "Cleaning time", "cleaning_time", map[string]any{"device_class": "duration", "unit_of_measurement": "min", "state_class": "measurement"}},
	{"total_cleaned_area", "Total cleaned area", "totals.area", map[string]any{"device_class": "area", "unit_of_measurement": "m²", "state_class": "total_increasing", "entity_category": "diagnostic"}},
	{"total_cleaning_time", "Total cleaning time", "totals.minutes", map[string]any{"device_class": "duration", "unit_of_measurement": "min", "state_class": "total_increasing", "entity_category": "diagnostic"}},
	{"cleaning_count", "Cleaning count", "totals.cleanings", map[string]any{"state_class": "total_increasing", "entity_category": "diagnostic", "icon": "mdi:counter"}},
	{"station", "Station", "station_state", map[string]any{"icon": "mdi:home-import-outline"}},
}

// Station work (4/25) as seen on r2505; other codes show as "code N".
var stationNames = map[float64]string{0: "idle", 1: "washing", 2: "drying", 5: "washing"}

// Station buttons are announced only when the agent says the model has the hardware.
var buttons = []struct{ command, name, icon string }{
	{"empty", "Empty dustbin", "mdi:delete-empty"},
	{"wash_mop", "Wash mop", "mdi:water"},
	{"dry_start", "Start mop drying", "mdi:fan"},
	{"dry_stop", "Stop mop drying", "mdi:fan-off"},
}

type capabilities struct {
	Identity map[string]string `json:"identity"`
	Agent    struct {
		Version string `json:"version"`
	} `json:"agent"`
	Commands map[string]struct {
		Available bool `json:"available"`
	} `json:"commands"`
}

type topics struct{ base, discovery, node string }

func topicsFor(c config, did string) topics {
	base := strings.Trim(c.BaseTopic, "/")
	if base == "" {
		base = "dreame_local/" + did
	}
	disc := strings.Trim(c.DiscoveryPrefix, "/")
	if disc == "" {
		disc = "homeassistant"
	}
	return topics{base: base, discovery: disc, node: "dreame_" + did}
}

// discovery returns retained config messages; an empty payload removes an entity.
// camera: the camera module is installed, so its live view gets a camera entity and an on/off switch;
// light: it also has the fill light (camera 1.4+).
func discovery(t topics, caps capabilities, version string, parts []string, settings []setting, camera, light bool) map[string]string {
	id := caps.Identity
	model := id["model"]
	device := map[string]any{"identifiers": []string{t.node}, "name": "Dreame " + strings.TrimPrefix(model, "dreame.vacuum."),
		"manufacturer": "Dreame", "model": model, "sw_version": id["fw_ver"]}
	origin := map[string]any{"name": "Dreame Local MQTT", "sw_version": version}
	common := func(extra map[string]any) string {
		m := map[string]any{"availability_topic": t.base + "/availability", "device": device, "origin": origin}
		for k, v := range extra {
			m[k] = v
		}
		b, _ := json.Marshal(m)
		return string(b)
	}
	out := map[string]string{
		t.discovery + "/vacuum/" + t.node + "/config": common(map[string]any{
			"name": nil, "unique_id": t.node + "_vacuum", "state_topic": t.base + "/state", "command_topic": t.base + "/command",
			"set_fan_speed_topic": t.base + "/fan_speed", "fan_speed_list": fanSpeeds, "send_command_topic": t.base + "/send_command",
			// Rooms ("segments" in the state) for Home Assistant's area mapping and vacuum.clean_area.
			"clean_segments_command_topic": t.base + "/clean_segments",
			// Attributes named as xiaomi-vacuum-map-card's Tasshack/dreame-vacuum template reads them (card.go).
			"json_attributes_topic": t.base + "/attributes",
			"supported_features":    []string{"start", "pause", "stop", "return_home", "locate", "fan_speed", "send_command"},
		}),
		t.discovery + "/sensor/" + t.node + "/battery/config": common(map[string]any{
			"name": "Battery", "unique_id": t.node + "_battery", "state_topic": t.base + "/state", "value_template": "{{ value_json.battery }}",
			"device_class": "battery", "unit_of_measurement": "%", "state_class": "measurement",
		}),
		t.discovery + "/sensor/" + t.node + "/error/config": common(map[string]any{
			"name": "Error", "unique_id": t.node + "_error", "state_topic": t.base + "/state", "value_template": "{{ value_json.error_name or value_json.error }}",
			"entity_category": "diagnostic", "icon": "mdi:alert-circle-outline",
		}),
		t.discovery + "/image/" + t.node + "/map/config": common(map[string]any{
			"name": "Map", "unique_id": t.node + "_map", "image_topic": t.base + "/map", "content_type": "image/png",
			// calibration_points and rooms for map cards such as xiaomi-vacuum-map-card
			"json_attributes_topic": t.base + "/map_attributes",
		}),
		// The same picture as a camera: map cards take their map from a camera entity.
		t.discovery + "/camera/" + t.node + "/map/config": common(map[string]any{
			"name": "Map camera", "unique_id": t.node + "_map_camera", "topic": t.base + "/map",
			"json_attributes_topic": t.base + "/map_attributes",
		}),
	}
	for _, s := range sensors {
		m := map[string]any{"name": s.name, "unique_id": t.node + "_" + s.id, "state_topic": t.base + "/state", "value_template": "{{ value_json." + s.field + " }}"}
		for k, v := range s.extra {
			m[k] = v
		}
		out[t.discovery+"/sensor/"+t.node+"/"+s.id+"/config"] = common(m)
	}
	// The robot stopped on an error; the Error sensor also shows warnings while it works normally.
	out[t.discovery+"/binary_sensor/"+t.node+"/problem/config"] = common(map[string]any{
		"name": "Problem", "unique_id": t.node + "_problem", "state_topic": t.base + "/state", "device_class": "problem",
		"value_template": "{{ 'ON' if value_json.state == 'error' else 'OFF' }}"})
	out[t.discovery+"/binary_sensor/"+t.node+"/charging/config"] = common(map[string]any{
		"name": "Charging", "unique_id": t.node + "_charging", "state_topic": t.base + "/state", "device_class": "battery_charging",
		"value_template": "{{ 'ON' if value_json.charging else 'OFF' }}"})
	out[t.discovery+"/number/"+t.node+"/volume/config"] = common(map[string]any{
		"name": "Volume", "unique_id": t.node + "_volume", "state_topic": t.base + "/state", "value_template": "{{ value_json.volume }}",
		"command_topic": t.base + "/volume/set", "min": 0, "max": 100, "step": 1, "unit_of_measurement": "%", "entity_category": "config", "icon": "mdi:volume-high"})
	for _, w := range []string{"start", "end"} {
		out[t.discovery+"/time/"+t.node+"/dnd_"+w+"/config"] = common(map[string]any{
			"name": "Do not disturb " + w, "unique_id": t.node + "_dnd_" + w, "state_topic": t.base + "/state", "value_template": "{{ value_json.dnd_" + w + " }}",
			"command_topic": t.base + "/dnd_" + w + "/set", "entity_category": "config", "icon": "mdi:minus-circle-outline"})
	}
	// A finished run, from the agent's history: an automation trigger with mode, result, minutes and area.
	out[t.discovery+"/event/"+t.node+"/cleaning/config"] = common(map[string]any{
		"name": "Cleaning", "unique_id": t.node + "_cleaning", "state_topic": t.base + "/cleaning_event", "event_types": []string{"finished", "failed"},
		"icon": "mdi:robot-vacuum"})
	for _, name := range parts {
		out[t.discovery+"/sensor/"+t.node+"/"+name+"/config"] = common(map[string]any{
			"name": strings.ReplaceAll(strings.ToUpper(name[:1])+name[1:], "_", " "), "unique_id": t.node + "_" + name,
			"state_topic": t.base + "/state", "value_template": "{{ value_json.consumables." + name + " }}",
			"json_attributes_topic": t.base + "/state", "json_attributes_template": "{{ {'hours': value_json.consumable_hours." + name + "} | tojson }}",
			"unit_of_measurement": "%", "state_class": "measurement", "icon": "mdi:wrench-clock",
		})
		// Pressed after replacing or cleaning the part; the agent refuses parts the robot does not report.
		out[t.discovery+"/button/"+t.node+"/reset_"+name+"/config"] = common(map[string]any{
			"name": "Reset " + strings.ReplaceAll(name, "_", " "), "unique_id": t.node + "_reset_" + name,
			"command_topic": t.base + "/command", "payload_press": "reset_" + name, "entity_category": "config", "icon": "mdi:restore",
		})
	}
	for topic, payload := range settingDiscovery(t, common, settings) {
		out[topic] = payload
	}
	for _, b := range buttons {
		topic := t.discovery + "/button/" + t.node + "/" + b.command + "/config"
		out[topic] = ""
		if caps.Commands[b.command].Available {
			out[topic] = common(map[string]any{"name": b.name, "unique_id": t.node + "_" + b.command, "command_topic": t.base + "/command",
				"payload_press": b.command, "icon": b.icon})
		}
	}
	for _, m := range moves {
		topic := t.discovery + "/button/" + t.node + "/" + m.command + "/config"
		out[topic] = ""
		if caps.Commands["remote_move"].Available {
			out[topic] = common(map[string]any{"name": m.name, "unique_id": t.node + "_" + m.command, "command_topic": t.base + "/command",
				"payload_press": m.command, "icon": m.icon})
		}
	}
	cam, live, lamp := t.discovery+"/camera/"+t.node+"/front/config", t.discovery+"/switch/"+t.node+"/camera_live/config", t.discovery+"/switch/"+t.node+"/camera_light/config"
	out[cam], out[live], out[lamp] = "", "", ""
	if camera && light {
		out[lamp] = common(map[string]any{"name": "Live view light", "unique_id": t.node + "_camera_light", "command_topic": t.base + "/camera_light/set",
			"state_topic": t.base + "/camera_light", "icon": "mdi:flashlight"})
	}
	if camera {
		// Frames arrive only while the switch is on: an MQTT camera cannot tell when someone watches.
		out[cam] = common(map[string]any{"name": "Camera", "unique_id": t.node + "_camera", "topic": t.base + "/camera", "image_encoding": "b64",
			"json_attributes_topic": t.base + "/camera_status"})
		out[live] = common(map[string]any{"name": "Camera stream", "unique_id": t.node + "_camera_live", "command_topic": t.base + "/camera_live/set",
			"state_topic": t.base + "/camera_live", "icon": "mdi:cctv"})
	}
	return out
}

func statePayload(s map[string]any, segments map[string]string) string {
	m := map[string]any{"state": s["activity"], "battery": s["battery"], "error": s["error"], "error_name": s["error_name"], "station": s["station"], "status": s["status_name"]}
	if parts, ok := s["consumables"].(map[string]any); ok {
		left := map[string]any{}
		for name, v := range parts {
			if c, ok := v.(map[string]any); ok {
				left[name] = c["left"]
			}
		}
		m["consumables"] = left
	}
	if v, ok := s["suction"].(float64); ok && int(v) >= 0 && int(v) < len(fanSpeeds) {
		m["fan_speed"] = fanSpeeds[int(v)]
	}
	if len(segments) > 0 {
		m["segments"] = segments
	}
	m["volume"], m["cleaned_area"], m["cleaning_time"], m["totals"] = s["volume"], s["cleaned_area"], s["cleaning_time"], s["stats"]
	if c, ok := s["charging"].(float64); ok {
		battery, _ := s["battery"].(float64)
		m["charging"] = c == 1 && battery < 100 // 1 is "on the dock", also when full
	}
	if v, ok := s["station"].(float64); ok {
		name, known := stationNames[v]
		if !known {
			name = fmt.Sprintf("code %v", v)
		}
		m["station_state"] = name
	}
	if d, ok := s["dnd"].(map[string]any); ok {
		m["dnd_start"], m["dnd_end"] = fmt.Sprint(d["start"], ":00"), fmt.Sprint(d["end"], ":00")
	}
	if parts, ok := s["consumables"].(map[string]any); ok {
		hours := map[string]any{}
		for name, v := range parts {
			if c, ok := v.(map[string]any); ok {
				hours[name] = c["hours"]
			}
		}
		m["consumable_hours"] = hours
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// consumableNames lists the consumables the agent reports, sorted, as discovery needs them.
func consumableNames(s map[string]any) []string {
	parts, _ := s["consumables"].(map[string]any)
	names := make([]string, 0, len(parts))
	for name := range parts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// commandFor maps an MQTT message to an agent command.
func commandFor(t topics, msg message, suction int) (map[string]any, error) {
	payload := strings.TrimSpace(msg.Payload)
	switch msg.Topic {
	case t.base + "/command":
		names := map[string]string{"start": "start", "pause": "pause", "stop": "stop", "return_to_base": "dock", "locate": "locate"}
		for _, b := range buttons {
			names[b.command] = b.command
		}
		if name, ok := names[payload]; ok {
			return map[string]any{"name": name}, nil
		}
		for _, m := range moves {
			if payload == m.command {
				return map[string]any{"name": "remote_move", "velocity": m.velocity, "rotation": m.rotation}, nil
			}
		}
		if part, ok := strings.CutPrefix(payload, "reset_"); ok && consumableRe.MatchString(part) {
			return map[string]any{"name": "reset_consumable", "consumable": part}, nil
		}
	case t.base + "/clean_segments": // ["3", "4"] from vacuum.clean_area
		var ids []string
		if json.Unmarshal([]byte(payload), &ids) == nil && len(ids) > 0 {
			rooms := []int{}
			for _, id := range ids {
				n, err := strconv.Atoi(id)
				if err != nil {
					return nil, fmt.Errorf("room id %q", id)
				}
				rooms = append(rooms, n)
			}
			return map[string]any{"name": "clean_rooms", "rooms": rooms, "repeats": 1, "suction": suction, "water": 2}, nil
		}
	case t.base + "/volume/set":
		if n, err := strconv.ParseFloat(payload, 64); err == nil && n >= 0 && n <= 100 {
			return map[string]any{"name": "set_volume", "value": int(n)}, nil
		}
	case t.base + "/fan_speed":
		for i, f := range fanSpeeds {
			if strings.EqualFold(f, payload) { // older lower-case names keep working
				return map[string]any{"name": "set_suction", "value": i}, nil
			}
		}
	case t.base + "/send_command":
		// Home Assistant's MQTT vacuum sends the bare command, or {"command": ..., <dict params merged in>}
		// (list params are dropped by Home Assistant). A nested "params" object is accepted too.
		var req map[string]any
		if json.Unmarshal([]byte(payload), &req) != nil {
			req = map[string]any{"command": payload}
		}
		name, _ := req["command"].(string)
		params, nested := req["params"].(map[string]any)
		if !nested {
			params = req
		}
		if name != "" {
			cmd := map[string]any{"name": name}
			for k, v := range params {
				if k != "name" && k != "command" {
					cmd[k] = v
				}
			}
			if name == "clean_zone" || name == "clean_spot" || name == "clean_rooms" {
				// The map card sends room ids as strings and may send fractional coordinates.
				for _, k := range []string{"rooms", "zones", "points"} {
					if v, ok := cmd[k]; ok {
						cmd[k] = integers(v)
					}
				}
				defaults := map[string]any{"repeats": 1, "suction": suction, "water": 2}
				for k, v := range defaults {
					if _, ok := cmd[k]; !ok {
						cmd[k] = v
					}
				}
			}
			return cmd, nil
		}
	}
	return nil, fmt.Errorf("unsupported %s payload %q", msg.Topic, payload)
}

// integers rounds numbers and numeric strings inside nested lists; anything else is left for the agent to reject.
// dndCommand changes one end of the do-not-disturb window, keeping the rest as the robot has it.
func dndCommand(state map[string]any, which, payload string) (map[string]any, error) {
	d, ok := state["dnd"].(map[string]any)
	if !ok {
		return nil, errors.New("do-not-disturb settings not known yet")
	}
	clock := strings.TrimSpace(payload)
	cmd := map[string]any{"name": "set_dnd", "enabled": d["enabled"] == true, "start": d["start"], "end": d["end"]}
	cmd[which] = clock[:min(5, len(clock))] // Home Assistant sends HH:MM:SS, the agent takes HH:MM
	return cmd, nil
}

func integers(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = integers(e)
		}
		return out
	case float64:
		return int(math.Round(x))
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
			return int(math.Round(f))
		}
	}
	return v
}

// ---- service

func (e env) run(ctx context.Context, version string) {
	delay := 5 * time.Second
	for ctx.Err() == nil {
		c, mtime := e.loadConfig()
		var err error
		if !c.Enabled || c.Host == "" {
			e.setStatus("not_configured", "", "")
		} else {
			started := time.Now()
			err = e.session(ctx, c, mtime, version)
			if err != nil {
				log.Print(err)
				e.setStatus("error", net.JoinHostPort(c.Host, fmt.Sprint(c.Port)), err.Error())
			}
			if time.Since(started) > time.Minute {
				delay = 5 * time.Second
			}
		}
		// Wait for a config change or the retry delay.
		wait := time.After(delay)
		if err != nil && delay < time.Minute {
			delay *= 2
		}
		for changed := false; !changed && ctx.Err() == nil; {
			select {
			case <-ctx.Done():
			case <-wait:
				changed = err != nil
			case <-time.After(2 * time.Second):
				_, m := e.loadConfig()
				changed = !m.Equal(mtime)
			}
		}
	}
}

// session runs one broker connection until it fails, the config changes or ctx ends.
func (e env) session(ctx context.Context, c config, mtime time.Time, version string) error {
	var caps capabilities
	if err := e.get(ctx, "/api/v1/capabilities", &caps); err != nil {
		return fmt.Errorf("agent capabilities: %w", err)
	}
	did := caps.Identity["did"]
	if did == "" {
		return errors.New("agent did not report the robot id")
	}
	t := topicsFor(c, did)
	broker := net.JoinHostPort(c.Host, fmt.Sprint(c.Port))
	e.setStatus("connecting", broker, "")
	client, err := dialMQTT(mqttOptions{Addr: broker, ClientID: t.node + "_local", Username: c.Username, Password: c.Password,
		WillTopic: t.base + "/availability", WillPayload: "offline", KeepAlive: 60 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()
	// Camera entities only while the camera module is installed (and its status answers).
	var cam struct {
		Light *bool `json:"light"` // camera 1.4+
	}
	hasCamera := func() bool {
		cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		cam.Light = nil
		return e.post(cctx, "/api/v1/m/camera/status", nil, &cam) == nil
	}
	camera := hasCamera()
	for topic, payload := range discovery(t, caps, version, nil, nil, camera, cam.Light != nil) {
		if err := client.Publish(topic, payload, true); err != nil {
			return err
		}
	}
	subscriptions := []string{t.base + "/command", t.base + "/fan_speed", t.base + "/send_command", t.base + "/setting/+/set", t.base + "/clean_segments", t.base + "/camera_live/set", t.base + "/camera_light/set",
		t.base + "/volume/set", t.base + "/dnd_start/set", t.base + "/dnd_end/set"}
	if err := client.Subscribe(subscriptions...); err != nil {
		return err
	}
	e.setStatus("connected", broker, "")
	log.Printf("connected to %s as %s", broker, t.base)

	sctx, stop := context.WithCancel(ctx)
	defer stop()
	snaps := make(chan *snapshot)
	go e.events(sctx, snaps)
	availability, announced, names := "", "", []string(nil)
	suction := 1                    // the robot's current suction, used for map card commands that carry none
	segments := map[string]string{} // room id -> name from the saved map
	var settings []setting
	var state map[string]any
	lastAttributes := ""
	publishAttributes := func() error {
		a := vacuumAttributes(state, settings)
		if a == lastAttributes {
			return nil
		}
		lastAttributes = a
		return client.Publish(t.base+"/attributes", a, true)
	}
	announce := func() error {
		for topic, payload := range discovery(t, caps, version, names, settings, camera, cam.Light != nil) {
			if err := client.Publish(topic, payload, true); err != nil {
				return err
			}
		}
		return nil
	}
	// Finished runs from the agent's history, published as events; the newest run at connect is only remembered.
	lastRun, historyRead := "", false
	checkHistory := func() error {
		hctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		var runs []map[string]any
		if e.get(hctx, "/api/v1/history", &runs) != nil {
			return nil
		}
		newest := ""
		if len(runs) > 0 {
			newest = fmt.Sprint(runs[0]["start"])
		}
		seen := historyRead
		historyRead = true
		if newest == lastRun {
			return nil
		}
		lastRun = newest
		if !seen {
			return nil
		}
		ev := runs[0]
		ev["event_type"] = "finished"
		if ev["result"] == "error" {
			ev["event_type"] = "failed"
		}
		b, _ := json.Marshal(ev)
		return client.Publish(t.base+"/cleaning_event", string(b), false)
	}
	var stopLive context.CancelFunc
	defer func() {
		if stopLive != nil {
			stopLive()
		}
	}()
	setLive := func(on bool) error {
		if stopLive != nil {
			stopLive()
			stopLive = nil
		}
		state := "OFF"
		if on && camera {
			lctx, cancel := context.WithTimeout(sctx, cameraFor)
			stopLive, state = cancel, "ON"
			go e.cameraLive(lctx, client, t)
		}
		return client.Publish(t.base+"/camera_live", state, true)
	}
	publishLight := func() error {
		if cam.Light == nil {
			return nil
		}
		return client.Publish(t.base+"/camera_light", map[bool]string{true: "ON", false: "OFF"}[*cam.Light], true)
	}
	// Settings come from the agent (0.22+); an older agent just has none.
	settingsKey := ""
	readSettings := func() error {
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		var list []setting
		if err := e.get(rctx, "/api/v1/settings", &list); err != nil {
			return nil
		}
		settings = list
		ids := []string{}
		if camera = hasCamera(); camera {
			ids = append(ids, "+camera") // installing or removing the camera module re-announces
			if cam.Light != nil {
				ids = append(ids, "+light")
			}
		} else if stopLive != nil {
			if err := setLive(false); err != nil { // the module was removed while streaming
				return err
			}
		}
		if err := publishLight(); err != nil {
			return err
		}
		for _, s := range list {
			ids = append(ids, s.ID)
		}
		if key := strings.Join(ids, ","); key != settingsKey {
			settingsKey = key
			if err := announce(); err != nil {
				return err
			}
		}
		if err := client.Publish(t.base+"/settings", settingsPayload(list), true); err != nil {
			return err
		}
		return publishAttributes()
	}
	settingsTick := time.NewTicker(settingsEvery)
	defer settingsTick.Stop()
	settingsSoon := time.NewTimer(time.Hour)
	settingsSoon.Stop()
	defer settingsSoon.Stop()
	publishAvailability := func(v string) error {
		if v == availability {
			return nil
		}
		availability = v
		return client.Publish(t.base+"/availability", v, true)
	}
	check := time.NewTicker(2 * time.Second)
	defer check.Stop()
	// The map picture: the agent renders it, published only when it changed.
	mapTick := time.NewTicker(mapEvery)
	defer mapTick.Stop()
	lastMap := ""
	publishMap := func() error {
		mctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		img, err := e.raw(mctx, "/api/v1/map.png")
		if err != nil || string(img) == lastMap {
			return nil // no saved map yet, or unchanged
		}
		lastMap = string(img)
		var card map[string]any
		if e.get(mctx, "/api/v1/map", &card) == nil {
			rooms, _ := card["rooms"].(map[string]any)
			segments = map[string]string{}
			for id, r := range rooms {
				if room, ok := r.(map[string]any); ok {
					segments[id] = fmt.Sprint(room["name"])
				}
			}
			b, _ := json.Marshal(card)
			if err := client.Publish(t.base+"/map_attributes", string(b), true); err != nil {
				return err
			}
		}
		return client.Publish(t.base+"/map", lastMap, true)
	}
	if err := setLive(false); err != nil {
		return err
	}
	if err := publishMap(); err != nil {
		return err
	}
	if err := checkHistory(); err != nil {
		return err
	}
	if err := readSettings(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			_ = client.Publish(t.base+"/availability", "offline", true)
			return nil
		case <-client.Done():
			return fmt.Errorf("broker connection lost: %v", client.err)
		case s := <-snaps:
			if s == nil || s.Stale || len(s.State) == 0 {
				err = publishAvailability("offline")
			} else {
				// Consumables are known only from the state; announce them once they appear.
				if names = consumableNames(s.State); strings.Join(names, ",") != announced {
					announced = strings.Join(names, ",")
					if err = announce(); err != nil {
						return err
					}
				}
				if v, ok := s.State["suction"].(float64); ok && v >= 0 && v <= 3 {
					suction = int(v)
				}
				state = s.State
				if err = client.Publish(t.base+"/state", statePayload(s.State, segments), true); err == nil {
					err = publishAttributes()
				}
				if err == nil {
					err = publishAvailability("online")
				}
			}
			if err != nil {
				return err
			}
		case msg, ok := <-client.Messages:
			if !ok {
				continue // Done() fires next
			}
			if msg.Topic == t.base+"/camera_light/set" {
				lctx, cancel := context.WithTimeout(ctx, 20*time.Second)
				err := e.post(lctx, "/api/v1/m/camera/light", map[string]any{"on": strings.EqualFold(strings.TrimSpace(msg.Payload), "ON")}, &cam)
				cancel()
				if err != nil {
					log.Printf("camera light: %v", err)
				}
				if err := publishLight(); err != nil { // the module's value, also after a refusal
					return err
				}
				continue
			}
			if msg.Topic == t.base+"/camera_live/set" {
				if err := setLive(strings.EqualFold(strings.TrimSpace(msg.Payload), "ON")); err != nil {
					return err
				}
				continue
			}
			cmd, err := commandFor(t, msg, suction)
			if w, ok := strings.CutPrefix(msg.Topic, t.base+"/dnd_"); ok && (w == "start/set" || w == "end/set") {
				cmd, err = dndCommand(state, strings.TrimSuffix(w, "/set"), msg.Payload)
			}
			if id, ok := settingTopic(t, msg.Topic); ok {
				cmd, err = settingCommand(settings, id, strings.TrimSpace(msg.Payload))
				settingsSoon.Reset(3 * time.Second) // show the robot's value, also after a refusal
			}
			if err == nil {
				log.Printf("command %v from %s", cmd["name"], strings.TrimPrefix(msg.Topic, t.base+"/"))
				cctx, cancel := context.WithTimeout(ctx, 45*time.Second)
				err = e.post(cctx, "/api/v1/command", cmd, nil)
				cancel()
			}
			if err != nil {
				log.Print(err)
			}
		case <-settingsTick.C:
			if err := readSettings(); err != nil {
				return err
			}
		case <-settingsSoon.C:
			if err := readSettings(); err != nil {
				return err
			}
		case <-mapTick.C:
			if err := publishMap(); err != nil {
				return err
			}
			if err := checkHistory(); err != nil {
				return err
			}
		case <-check.C:
			if _, m := e.loadConfig(); !m.Equal(mtime) {
				_ = client.Publish(t.base+"/availability", "offline", true)
				return nil
			}
		}
	}
}
