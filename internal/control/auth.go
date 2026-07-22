package control

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
)

// LoadToken reads a single-line bearer token from path. Empties and missing
// files are errors so the operator can't accidentally end up with an empty
// allowed-token set (which would mean "no auth"). Whitespace around the token
// is trimmed.
func LoadToken(path string) (string, error) {
	//nolint:gosec // G304: path is operator-supplied via -control-token-file, not user input.
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read control token: %w", err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", errors.New("control token file is empty")
	}
	return tok, nil
}

// IsLoopbackBind reports whether a host:port listen string targets the
// loopback interface. Used by main.go to decide whether the bearer-token
// check is the deployment's auth boundary, and by NewServer to short-circuit
// the middleware when loopback alone suffices.
func IsLoopbackBind(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Treat unparseable as "not loopback" — fail closed; the operator
		// gets the auth check if their bind is malformed.
		return false
	}
	if host == "localhost" || host == "" {
		// Empty host (":9876") binds all interfaces, NOT loopback. Only
		// "localhost" string maps to loopback in this branch.
		return host == "localhost"
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// bearerAuth requires an Authorization: Bearer <token> header on Connect RPC
// and dashboard data paths. Health, metrics, and the static dashboard shell
// stay open; the shell contains no fleet data and sends the token to the API
// from browser sessionStorage.
func bearerAuth(next http.Handler, token string) http.Handler {
	if token == "" {
		return next
	}
	expected := "Bearer " + token
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publicControlPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		got := r.Header.Get("Authorization")
		// Constant-time compare so a remote attacker can't time-side-channel
		// the token byte-by-byte.
		authorized := subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1 ||
			(r.URL.Path == "/dashboard/ws" && websocketBearerMatches(r, token))
		if !authorized {
			w.Header().Set("WWW-Authenticate", `Bearer realm="fjb-control"`)
			w.WriteHeader(http.StatusUnauthorized)
			// Tiny JSON body so curl --fail and Connect clients both see a
			// useful message. Connect maps HTTP 401 to CodeUnauthenticated
			// regardless of body, so the format is informational here.
			_, _ = w.Write([]byte(`{"error":"unauthenticated"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func publicControlPath(path string) bool {
	if path == "/" || path == "/healthz" || path == "/metrics" || path == "/dashboard" {
		return true
	}
	switch path {
	case "/dashboard/", "/dashboard/index.html", "/dashboard/app.css", "/dashboard/app.js":
		return true
	default:
		return false
	}
}

func websocketBearerMatches(r *http.Request, token string) bool {
	const prefix = "fjb-bearer."
	for protocol := range strings.SplitSeq(r.Header.Get("Sec-WebSocket-Protocol"), ",") {
		protocol = strings.TrimSpace(protocol)
		if !strings.HasPrefix(protocol, prefix) {
			continue
		}
		decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(protocol, prefix))
		if err == nil && subtle.ConstantTimeCompare(decoded, []byte(token)) == 1 {
			return true
		}
	}
	return false
}
