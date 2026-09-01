// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
package hrotti

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
)

//loggers
var (
	INFO     *log.Logger
	PROTOCOL *log.Logger
	ERROR    *log.Logger
	DEBUG    *log.Logger
)

//The default output for all the loggers is set to ioutil.Discard
func init() {
	INFO = log.New(ioutil.Discard, "", 0)
	PROTOCOL = log.New(ioutil.Discard, "", 0)
	ERROR = log.New(ioutil.Discard, "", 0)
	DEBUG = log.New(ioutil.Discard, "", 0)
}

//ListenerConfig is a struct containing a URL
type ListenerConfig struct {
	URL *url.URL
}

//NewListenerConfig returns a pointer to a ListenerConfig prepared to listen
//on the URL specified as rawURL. The caller MUST check both the returned error
//and the result: nil ListenerConfig on error. Callers that previously relied on
//"nil-means-error" still work; new callers should switch to the two-return form.
func NewListenerConfig(rawURL string) *ListenerConfig {
	lc, _ := NewListenerConfigWithError(rawURL)
	return lc
}

// NewListenerConfigWithError is the safer two-return constructor. It returns
// a non-nil error when rawURL cannot be parsed as a URL.
func NewListenerConfigWithError(rawURL string) (*ListenerConfig, error) {
	listenerURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if listenerURL.Scheme != "tcp" && listenerURL.Scheme != "ws" {
		return nil, &url.Error{Op: "parse", URL: rawURL, Err: fmt.Errorf("unsupported listener scheme %q (only tcp and ws)", listenerURL.Scheme)}
	}
	return &ListenerConfig{URL: listenerURL}, nil
}
