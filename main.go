// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	. "github.com/FasterEdge/MqttBrokerCore/broker"
)

// resolveListener returns a listener for the HROTTI_URL environment variable,
// falling back to the default tcp://0.0.0.0:1883 when the env var is empty or
// unparsable. NewListenerConfig returns nil for empty/invalid URLs, so callers
// must nil-check before dereferencing (see main).
func resolveListener() *ListenerConfig {
	listener := NewListenerConfig(os.Getenv("HROTTI_URL"))
	if listener == nil || listener.URL.Host == "" {
		listener = NewListenerConfig("tcp://0.0.0.0:1883")
	}
	return listener
}

func createConfig() BrokerConfig {
	configFile := flag.String("conf", "", "A configuration file")

	flag.Parse()

	var config BrokerConfig
	config.ListenerEntries = make(map[string]*ListenerEntry)
	config.Listeners = make(map[string]*ListenerConfig)

	if *configFile == "" {
		config.Listeners["envconfig"] = resolveListener()
		config.MaxQueueDepth = 100
	} else {
		fmt.Println("Reading config file", *configFile)
		err := ParseConfig(*configFile, &config)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("%s\n", err.Error()))
		}
	}
	config.SetLogTargets()
	return config
}

func main() {
	config := createConfig()

	//r := &RedisPersistence{Server: ":6379"}
	r := &MemoryPersistence{}
	h := NewHrotti(config.MaxQueueDepth, r)

	for name, listener := range config.Listeners {
		h.AddListener(name, listener)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	h.Stop()
}
