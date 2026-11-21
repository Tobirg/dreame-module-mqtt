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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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
	req, _ := http.NewRequestWithContext(ctx, "GET", e.api+path, nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	res, err := e.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	var body struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&body); err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("agent: %s", body.Error)
	}
	return json.Unmarshal(body.Result, v)
}

func (e env) command(ctx context.Context, cmd map[string]any) error {
	b, _ := json.Marshal(cmd)
	req, _ := http.NewRequestWithContext(ctx, "POST", e.api+"/api/v1/command", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := e.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		var body struct{ Error string }
		_ = json.NewDecoder(io.LimitReader(res.Body, 65536)).Decode(&body)
		return fmt.Errorf("agent refused %v: %s", cmd["name"], body.Error)
	}
	return nil
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

var fanSpeeds = []string{"quiet", "standard", "strong", "turbo"}

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
func discovery(t topics, caps capabilities, version string, parts []string) map[string]string {
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
			"supported_features": []string{"start", "pause", "stop", "return_home", "locate", "fan_speed", "send_command"},
		}),
		t.discovery + "/sensor/" + t.node + "/battery/config": common(map[string]any{
			"name": "Battery", "unique_id": t.node + "_battery", "state_topic": t.base + "/state", "value_template": "{{ value_json.battery }}",
			"device_class": "battery", "unit_of_measurement": "%", "state_class": "measurement",
		}),
		t.discovery + "/sensor/" + t.node + "/error/config": common(map[string]any{
			"name": "Error", "unique_id": t.node + "_error", "state_topic": t.base + "/state", "value_template": "{{ value_json.error_name or value_json.error }}",
			"entity_category": "diagnostic", "icon": "mdi:alert-circle-outline",
		}),
	}
	for _, name := range parts {
		out[t.discovery+"/sensor/"+t.node+"/"+name+"/config"] = common(map[string]any{
			"name": strings.ReplaceAll(strings.ToUpper(name[:1])+name[1:], "_", " "), "unique_id": t.node + "_" + name,
			"state_topic": t.base + "/state", "value_template": "{{ value_json.consumables." + name + " }}",
			"unit_of_measurement": "%", "state_class": "measurement", "icon": "mdi:wrench-clock",
		})
	}
	for _, b := range buttons {
		topic := t.discovery + "/button/" + t.node + "/" + b.command + "/config"
		out[topic] = ""
		if caps.Commands[b.command].Available {
			out[topic] = common(map[string]any{"name": b.name, "unique_id": t.node + "_" + b.command, "command_topic": t.base + "/command",
				"payload_press": b.command, "icon": b.icon})
		}
	}
	return out
}

func statePayload(s map[string]any) string {
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
func commandFor(t topics, msg message) (map[string]any, error) {
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
	case t.base + "/fan_speed":
		for i, f := range fanSpeeds {
			if f == payload {
				return map[string]any{"name": "set_suction", "value": i}, nil
			}
		}
	case t.base + "/send_command":
		// Home Assistant sends {"command": ..., "params": {...}}, or the bare command without params.
		var req struct {
			Command string         `json:"command"`
			Params  map[string]any `json:"params"`
		}
		if json.Unmarshal([]byte(payload), &req) != nil {
			req.Command = payload
		}
		if req.Command != "" {
			cmd := map[string]any{"name": req.Command}
			for k, v := range req.Params {
				if k != "name" {
					cmd[k] = v
				}
			}
			return cmd, nil
		}
	}
	return nil, fmt.Errorf("unsupported %s payload %q", msg.Topic, payload)
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
	for topic, payload := range discovery(t, caps, version, nil) {
		if err := client.Publish(topic, payload, true); err != nil {
			return err
		}
	}
	if err := client.Subscribe(t.base+"/command", t.base+"/fan_speed", t.base+"/send_command"); err != nil {
		return err
	}
	e.setStatus("connected", broker, "")
	log.Printf("connected to %s as %s", broker, t.base)

	sctx, stop := context.WithCancel(ctx)
	defer stop()
	snaps := make(chan *snapshot)
	go e.events(sctx, snaps)
	availability, announced := "", ""
	publishAvailability := func(v string) error {
		if v == availability {
			return nil
		}
		availability = v
		return client.Publish(t.base+"/availability", v, true)
	}
	check := time.NewTicker(2 * time.Second)
	defer check.Stop()
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
				if names := consumableNames(s.State); strings.Join(names, ",") != announced {
					announced = strings.Join(names, ",")
					for topic, payload := range discovery(t, caps, version, names) {
						if err = client.Publish(topic, payload, true); err != nil {
							return err
						}
					}
				}
				if err = client.Publish(t.base+"/state", statePayload(s.State), true); err == nil {
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
			cmd, err := commandFor(t, msg)
			if err == nil {
				cctx, cancel := context.WithTimeout(ctx, 45*time.Second)
				err = e.command(cctx, cmd)
				cancel()
			}
			if err != nil {
				log.Print(err)
			}
		case <-check.C:
			if _, m := e.loadConfig(); !m.Equal(mtime) {
				_ = client.Publish(t.base+"/availability", "offline", true)
				return nil
			}
		}
	}
}
