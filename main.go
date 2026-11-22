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
		// auth 段结构性校验失败必须终止启动: 带着错的 ACL 跑起来等于
		// 以为有访问控制实际没有。
		if err := config.validateAuthEntries(); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("invalid auth config: %s\n", err.Error()))
			os.Exit(2)
		}
	}
	if !config.AuthConfigured() {
		// 未配置 auth: broker 处于 fail-closed, 所有 CONNECT 都会被拒。
		// 这里大声说出来, 免得使用者以为"没报错就是能连"。
		fmt.Println("WARNING: no auth section in config; broker runs fail-closed (all CONNECTs refused).")
		fmt.Println("         Add an \"auth\" section with \"allowed\" credentials, or set \"allowAnonymous\": true to disable access control explicitly.")
	}
	config.SetLogTargets()
	return config
}

func main() {
	config := createConfig()

	//r := &RedisPersistence{Server: ":6379"}
	r := &MemoryPersistence{}
	h := NewHrotti(config.MaxQueueDepth, r)

	if err := config.ApplyAuth(h); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to apply auth config: %s\n", err.Error()))
		os.Exit(2)
	}

	for name, listener := range config.Listeners {
		h.AddListener(name, listener)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	h.Stop()
}
