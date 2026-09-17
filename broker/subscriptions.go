// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package hrotti

import "strings"

// Add a subscription for a client, taking an array of topics to subscribe to and an associated
// slice of QoS values for the topics, return a slice of byte values indicating the granted
// QoS values in topics order.
func (h *Hrotti) AddSubscription(c *Client, topics []string, qoss []byte) []byte {
	//this is the slice we'll return and needs to be the same length as the input QoS' slice
	rQos := make([]byte, len(qoss))

	//for every topic in the topics slice, also get the index number of the topic...
	for i, topic := range topics {
		// 非法 QoS(>2, 保留值 3 或更大)与非法主题过滤器(空段/#位置/+
		// 非完整段)按 MQTT 3.1.1 §3.8.4/§4.7 返回 SUBACK 0x80(失败),
		// 不注册订阅——旧实现透传任意 qos 值回显给客户端(协议状态机可被
		// 畸形 SUBSCRIBE 打乱)。
		if qoss[i] > 2 || !isValidTopicFilter(topic) {
			rQos[i] = 0x80
			continue
		}
		h.AddSub(c.clientID, topic, qoss[i])
		rQos[i] = qoss[i]
	}
	//return the slice of granted QoS values.
	return rQos
}

// isValidTopicFilter 校验 MQTT 3.1.1 主题过滤器: 非空、无空段、'#' 仅允许
// 作为最后一个完整段、'+' 仅允许作为完整段、无空字符。
func isValidTopicFilter(topic string) bool {
	if topic == "" || len(topic) > 65535 {
		return false
	}
	if strings.ContainsRune(topic, '\u0000') {
		return false
	}
	segs := strings.Split(topic, "/")
	for i, seg := range segs {
		if seg == "" {
			return false // 空段("a//b" 或前导/尾随 '/')
		}
		if strings.Contains(seg, "#") {
			// '#' 必须独占一段且是最后一段("a/#" 合法, "a#"、"#/a" 非法)
			if seg != "#" || i != len(segs)-1 {
				return false
			}
		}
		if strings.Contains(seg, "+") {
			// '+' 必须独占一段("a/+" 合法, "a+" 非法)
			if seg != "+" {
				return false
			}
		}
	}
	return true
}

// isValidTopicName 校验 MQTT 3.1.1 主题名(用于 PUBLISH/遗嘱): 非空、无空字符、
// 且不含通配符 '#'/'+'(主题名与主题过滤器的关键区别)。主题名允许含 '/' 与空段。
func isValidTopicName(topic string) bool {
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

func (h *Hrotti) RemoveSubscription(c *Client, topic string) bool {
	h.DeleteSub(c.clientID, topic)
	return true
}
