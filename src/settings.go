package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// setting is one entry of the agent's GET /api/v1/settings (agent 0.22+): the panel's catalog
// entries this robot has, with their current value.
type setting struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Kind  string `json:"kind"` // bool, select, number
	// Service and cloud-only settings (agent 0.41+): announced, but disabled until enabled in Home Assistant
	// (applies when Home Assistant first creates the entity).
	Advanced bool   `json:"advanced"`
	Auto     string `json:"auto"`
	Options  []struct {
		Value any    `json:"value"`
		Label string `json:"label"`
	} `json:"options"`
	Min   *float64 `json:"min"`
	Max   *float64 `json:"max"`
	Step  *float64 `json:"step"`
	Value any      `json:"value"`
}

var settingIDRe = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// settingDiscovery announces each setting as a switch, select or number under the device.
func settingDiscovery(t topics, common func(map[string]any) string, list []setting) map[string]string {
	out := map[string]string{}
	for _, s := range list {
		if !settingIDRe.MatchString(s.ID) {
			continue
		}
		m := map[string]any{"name": s.Label, "unique_id": t.node + "_setting_" + s.ID, "entity_category": "config",
			"state_topic": t.base + "/settings", "value_template": "{{ value_json." + s.ID + " }}",
			"command_topic": t.base + "/setting/" + s.ID + "/set"}
		if s.Advanced {
			m["enabled_by_default"] = false
		}
		component := "switch"
		switch s.Kind {
		case "bool":
			m["payload_on"], m["payload_off"] = "ON", "OFF"
		case "select":
			component = "select"
			labels := []string{}
			for _, o := range s.Options {
				labels = append(labels, o.Label)
			}
			m["options"] = labels
		case "number":
			component = "number"
			m["min"], m["max"], m["step"], m["mode"] = s.Min, s.Max, s.Step, "box"
		default:
			continue
		}
		out[t.discovery+"/"+component+"/"+t.node+"/setting_"+s.ID+"/config"] = common(m)
	}
	return out
}

// settingsPayload is the retained state: ON/OFF, the option label, or the number.
func settingsPayload(list []setting) string {
	m := map[string]any{}
	for _, s := range list {
		switch s.Kind {
		case "bool":
			on := s.Value == true
			if n, ok := s.Value.(float64); ok {
				on = n > 0
			}
			m[s.ID] = map[bool]string{true: "ON", false: "OFF"}[on]
		case "select":
			v := s.Value
			// The app keeps "off" auto settings as minus the last level; they show as option 0.
			if n, ok := v.(float64); ok && s.Auto != "" && n < 0 {
				v = float64(0)
			}
			m[s.ID] = fmt.Sprint(v)
			for _, o := range s.Options {
				if fmt.Sprint(o.Value) == fmt.Sprint(v) {
					m[s.ID] = o.Label
				}
			}
		default:
			m[s.ID] = s.Value
		}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// settingCommand turns a Home Assistant setting message into the agent's set_setting command.
func settingCommand(list []setting, id, payload string) (map[string]any, error) {
	for _, s := range list {
		if s.ID != id {
			continue
		}
		var to any
		switch s.Kind {
		case "bool":
			switch payload {
			case "ON":
				to = true
			case "OFF":
				to = false
			}
		case "select":
			for _, o := range s.Options {
				if o.Label == payload {
					to = o.Value
				}
			}
		case "number":
			if n, err := strconv.ParseFloat(strings.TrimSpace(payload), 64); err == nil {
				to = n
			}
		}
		if to == nil {
			return nil, fmt.Errorf("invalid value %q for %s", payload, id)
		}
		return map[string]any{"name": "set_setting", "setting": id, "to": to}, nil
	}
	return nil, fmt.Errorf("unknown setting %s", id)
}
