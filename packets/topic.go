// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package packets

import "strings"

// IsValidTopicName 校验 MQTT 3.1.1 主题名(用于 PUBLISH/遗嘱): 非空、无空字符、
// 且不含通配符 '#'/'+'(主题名与主题过滤器的关键区别)。主题名允许含 '/' 与空段。
func IsValidTopicName(topic string) bool {
	if topic == "" || len(topic) > 65535 {
		return false
	}
	if strings.ContainsRune(topic, '\u0000') {
		return false
	}
	if strings.ContainsAny(topic, "#+") {
		return false
	}
	return true
}
