// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package packets

import "strings"

// MaxTopicSegments 是主题名/过滤器允许的最大段数。broker/router.go 的订阅 bitmap
// (subBitmap) 按 MaxTopicSegments+1 层分配: 主题 N 段在 AddSub/DeliverMessage 里
// append 一个 "\u0000" 哨兵后按 i=0..N 索引 subBitmap[i], 故 subBitmap 层数必须
// > 最大段数。超出段数的主题在 bitmap 索引越界 panic(远程 DoS), 故在校验层拒绝。
const MaxTopicSegments = 31

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
	if strings.Count(topic, "/")+1 > MaxTopicSegments {
		return false
	}
	return true
}
