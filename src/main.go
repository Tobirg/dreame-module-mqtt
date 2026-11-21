// Dreame Local MQTT module: publishes the robot to an MQTT broker with Home Assistant
// discovery and turns MQTT commands into agent commands. One binary serves as the
// module handler ("mqtt-bridge <action>") and as its service ("mqtt-bridge run").
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var version = "1.0.0"

var topicRe = regexp.MustCompile(`^[A-Za-z0-9_\-/]{1,100}$`)

func main() {
	e := env{data: os.Getenv("DREAME_MODULE_DATA"), api: os.Getenv("DREAME_API"), token: os.Getenv("DREAME_MODULE_TOKEN"), http: &http.Client{}}
	if len(os.Args) != 2 || e.data == "" {
		fmt.Fprintln(os.Stderr, "run by the Dreame Local agent: mqtt-bridge <action>")
		os.Exit(2)
	}
	if os.Args[1] == "run" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		e.run(ctx, version)
		return
	}
	result, err := e.action(os.Args[1], os.Stdin)
	if err != nil {
		b, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Println(string(b))
		os.Exit(1)
	}
	b, _ := json.Marshal(result)
	fmt.Println(string(b))
}

func (e env) action(name string, stdin io.Reader) (any, error) {
	switch name {
	case "check", "install", "status":
		return e.status(), nil
	case "configure":
		var req struct {
			config
			KeepPassword bool `json:"keep_password"`
		}
		d := json.NewDecoder(io.LimitReader(stdin, 65536))
		if err := d.Decode(&req); err != nil {
			return nil, errors.New("invalid settings")
		}
		c := req.config
		if req.KeepPassword {
			old, _ := e.loadConfig()
			c.Password = old.Password
		}
		if err := validate(&c); err != nil {
			return nil, err
		}
		if err := writeJSON(e.configPath(), c); err != nil {
			return nil, errors.New("cannot save settings")
		}
		return e.status(), nil
	case "test":
		// Checks the broker settings from the panel without saving them.
		c, _ := e.loadConfig()
		if c.Host == "" {
			return nil, errors.New("broker is not configured")
		}
		client, err := dialMQTT(mqttOptions{Addr: net.JoinHostPort(c.Host, fmt.Sprint(c.Port)), ClientID: fmt.Sprintf("dreame_local_test_%d", time.Now().Unix()),
			Username: c.Username, Password: c.Password, KeepAlive: 30 * time.Second})
		if err != nil {
			return nil, err
		}
		client.Close()
		return map[string]bool{"ok": true}, nil
	case "uninstall":
		return e.removeEntities(), nil
	}
	return nil, fmt.Errorf("unknown action %q", name)
}

func validate(c *config) error {
	c.Host = strings.TrimSpace(c.Host)
	if c.Port == 0 {
		c.Port = 1883
	}
	if c.Enabled && c.Host == "" {
		return errors.New("broker address is required")
	}
	if c.Host != "" && (strings.ContainsAny(c.Host, " /:@") && net.ParseIP(c.Host) == nil || len(c.Host) > 253) {
		return errors.New("broker address: host name or IP without port")
	}
	if c.Port < 1 || c.Port > 65535 {
		return errors.New("broker port must be 1-65535")
	}
	if len(c.Username) > 128 || len(c.Password) > 256 {
		return errors.New("user name or password too long")
	}
	for _, t := range []*string{&c.BaseTopic, &c.DiscoveryPrefix} {
		*t = strings.Trim(strings.TrimSpace(*t), "/")
		if *t != "" && (!topicRe.MatchString(*t) || strings.Contains(*t, "//")) {
			return errors.New("topics may contain letters, digits, '_', '-' and '/'")
		}
	}
	return nil
}

func (e env) status() map[string]any {
	c, _ := e.loadConfig()
	out := map[string]any{"version": version, "enabled": c.Enabled, "host": c.Host, "port": c.Port, "username": c.Username,
		"has_password": c.Password != "", "base_topic": c.BaseTopic, "discovery_prefix": c.DiscoveryPrefix}
	var st map[string]any
	if b, err := os.ReadFile(filepath.Join(e.data, "status.json")); err == nil && json.Unmarshal(b, &st) == nil {
		out["connection"] = st
	}
	return out
}

// removeEntities clears the retained discovery messages so Home Assistant drops the device.
func (e env) removeEntities() map[string]any {
	c, _ := e.loadConfig()
	if c.Host == "" {
		return map[string]any{"removed": false}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var caps capabilities
	if err := e.get(ctx, "/api/v1/capabilities", &caps); err != nil || caps.Identity["did"] == "" {
		return map[string]any{"removed": false, "warning": "agent unavailable; remove the device in Home Assistant by hand"}
	}
	t := topicsFor(c, caps.Identity["did"])
	client, err := dialMQTT(mqttOptions{Addr: net.JoinHostPort(c.Host, fmt.Sprint(c.Port)), ClientID: t.node + "_remove", Username: c.Username, Password: c.Password, KeepAlive: 30 * time.Second})
	if err != nil {
		return map[string]any{"removed": false, "warning": err.Error()}
	}
	defer client.Close()
	for topic := range discovery(t, caps, version) {
		_ = client.Publish(topic, "", true)
	}
	for _, s := range []string{"/state", "/availability"} {
		_ = client.Publish(t.base+s, "", true)
	}
	return map[string]any{"removed": true}
}
