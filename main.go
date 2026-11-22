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

// Version 是本程序的版本号, 与仓库根目录 VERSION 文件保持一致
// (1.0.20261123)。改动版本时两处必须同步。`--version` 输出该常量, 启动
// 日志也会打印它, 使 VERSION 文件不再是无人引用的事实来源。
const Version = "1.0.20261123"

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
	showVersion := flag.Bool("version", false, "Print version and exit")

	flag.Parse()

	if *showVersion {
		fmt.Println("MqttBrokerCore", Version)
		os.Exit(0)
	}

	var config BrokerConfig
	config.ListenerEntries = make(map[string]*ListenerEntry)
	config.Listeners = make(map[string]*ListenerConfig)

	if *configFile == "" {
		config.Listeners["envconfig"] = resolveListener()
		config.MaxQueueDepth = DefaultMaxQueueDepth
	} else {
		fmt.Println("Reading config file", *configFile)
		err := ParseConfig(*configFile, &config)
		if err != nil {
			// 配置装载失败必须终止启动: 旧实现只打印错误后继续, 带着
			// 空 Listeners 跑成一个什么都不监听的 broker, 或(在 scheme
			// 校验前)把 udp:// 静默开成 TCP。
			os.Stderr.WriteString(fmt.Sprintf("failed to parse config %s: %s\n", *configFile, err.Error()))
			os.Exit(2)
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
	// 启动日志带版本号, 使 VERSION 文件有可核对的运行时出口(与 --version
	// 输出同一常量, 不会各说各话)。
	fmt.Println("MqttBrokerCore", Version, "starting")

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
