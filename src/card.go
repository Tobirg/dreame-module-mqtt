package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

// cardYAML is a ready xiaomi-vacuum-map-card config: zone, point and room modes that call
// vacuum.send_command with dict params (Home Assistant's MQTT vacuum drops list params),
// and the robot's rooms from the agent's map card data. Entity ids are Home Assistant's
// defaults for the discovered device; renamed entities have to be fixed by hand.
func cardYAML(model string, card map[string]any) string {
	dev := strings.Trim(slugRe.ReplaceAllString(strings.ToLower("Dreame "+strings.TrimPrefix(model, "dreame.vacuum.")), "_"), "_")
	q := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	mode := func(name, icon, selection, command, params, extra string) string {
		return fmt.Sprintf(`  - name: %s
    icon: %s
    selection_type: %s
%s    repeats_type: EXTERNAL
    max_repeats: 3
    service_call_schema:
      service: vacuum.send_command
      service_data:
        entity_id: "[[entity_id]]"
        command: %s
        params: %s
`, name, icon, selection, extra, command, params)
	}
	var b strings.Builder
	fmt.Fprintf(&b, `type: custom:xiaomi-vacuum-map-card
entity: vacuum.%s
# Tiles and setting menus come from this platform's template: the module names its attributes and
# entities to match it, so new ones appear without changing this text. Map modes are below.
vacuum_platform: Tasshack/dreame-vacuum
map_source:
  camera: image.%s_map
calibration_source:
  camera: true
map_modes:
`, dev, dev)
	b.WriteString(mode("Зона", "mdi:select-drag", "MANUAL_RECTANGLE", "clean_zone",
		`{zones: "[[selection]]", repeats: "[[repeats]]"}`, "    coordinates_rounding: true\n    max_selections: 5\n"))
	b.WriteString(mode("Точка", "mdi:map-marker", "MANUAL_POINT", "clean_spot",
		`{points: [["[[point_x]]", "[[point_y]]"]], repeats: "[[repeats]]"}`, "    coordinates_rounding: true\n"))
	rooms, _ := card["rooms"].(map[string]any)
	if len(rooms) == 0 {
		return b.String()
	}
	b.WriteString(mode("Комнаты", "mdi:floor-plan", "ROOM", "clean_rooms",
		`{rooms: "[[selection]]", repeats: "[[repeats]]"}`, "    max_selections: 10\n"))
	b.WriteString("    predefined_selections:\n")
	ids := make([]string, 0, len(rooms))
	for id := range rooms {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { a, _ := strconv.Atoi(ids[i]); c, _ := strconv.Atoi(ids[j]); return a < c })
	for _, id := range ids {
		r, _ := rooms[id].(map[string]any)
		fmt.Fprintf(&b, "      - id: %s\n        label: {text: %s, x: %v, 'y': %v}\n        outline: %s\n", q(id), q(r["name"]), r["x"], r["y"], q(r["outline"]))
	}
	return b.String()
}

// Values the card's Tasshack/dreame-vacuum template translates, by the agent's setting values.
var attributeNames = map[string]map[float64]string{
	"cleaning_mode":  {0: "sweeping", 1: "mopping", 2: "sweeping and mopping", 3: "mopping after sweeping"},
	"mop_humidity":   {1: "slightly_dry", 2: "moist", 3: "wet"},
	"mop_wash_level": {0: "water saving", 1: "daily", 2: "deep"},
	"cleaning_route": {1: "standard", 2: "intensive", 3: "deep", 4: "quick"},
	"cleangenius":    {0: "off", 1: "routine cleaning", 2: "deep cleaning"},
}

// vacuumAttributes are the vacuum entity's attributes: the card builds its tiles from them, so
// whatever the robot reports shows up without listing it in the card config.
func vacuumAttributes(state map[string]any, settings []setting) string {
	m := map[string]any{}
	copyAs := func(to string, v any) {
		if v != nil {
			m[to] = v
		}
	}
	copyAs("status", state["status_name"])
	copyAs("battery_level", state["battery"])
	copyAs("cleaning_time", state["cleaning_time"])
	copyAs("cleaned_area", state["cleaned_area"])
	if stats, ok := state["stats"].(map[string]any); ok {
		copyAs("cleaning_count", stats["cleanings"])
		copyAs("total_cleaned_area", stats["area"])
		copyAs("total_cleaning_time", stats["minutes"])
	}
	for _, s := range settings {
		to := s.ID
		if to == "mop_humidity" {
			to = "mop_pad_humidity"
		}
		switch v := s.Value.(type) {
		case float64:
			if names, ok := attributeNames[s.ID]; ok {
				if name, known := names[v]; known {
					m[to] = name
				}
			} else if s.ID == "wetness_level" {
				copyAs(to, v)
			} else if s.ID == "tight_mopping" {
				copyAs(to, fmt.Sprint(v > 0))
			}
		case bool:
			if s.ID == "tight_mopping" {
				copyAs(to, fmt.Sprint(v))
			}
		}
	}
	b, _ := json.Marshal(m)
	return string(b)
}
