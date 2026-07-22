package hetzner

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"strings"
)

// withAuthorizedKey adds a cloud-init shell-script part that installs the
// orchestrator key for root. Rebuild does not accept Hetzner SSH-key IDs, so
// user-data is the one mechanism that works identically for cold creates and
// in-place resets.
func withAuthorizedKey(userData, authorizedKey string) (string, error) {
	authorizedKey = strings.TrimSpace(authorizedKey)
	if authorizedKey == "" {
		return userData, nil
	}
	sum := sha256.Sum256([]byte(userData + "\x00" + authorizedKey))
	boundary := fmt.Sprintf("fjb-%x", sum[:16])

	var out bytes.Buffer
	w := multipart.NewWriter(&out)
	if err := w.SetBoundary(boundary); err != nil {
		return "", fmt.Errorf("set cloud-init MIME boundary: %w", err)
	}
	cloudHeader := make(textproto.MIMEHeader)
	cloudHeader.Set("Content-Type", "text/cloud-config; charset=\"us-ascii\"")
	part, err := w.CreatePart(cloudHeader)
	if err != nil {
		return "", fmt.Errorf("create cloud-config MIME part: %w", err)
	}
	if _, err := part.Write([]byte(userData)); err != nil {
		return "", fmt.Errorf("write cloud-config MIME part: %w", err)
	}

	scriptHeader := make(textproto.MIMEHeader)
	scriptHeader.Set("Content-Type", "text/x-shellscript; charset=\"us-ascii\"")
	part, err = w.CreatePart(scriptHeader)
	if err != nil {
		return "", fmt.Errorf("create authorized-key MIME part: %w", err)
	}
	encodedKey := base64.StdEncoding.EncodeToString([]byte(authorizedKey))
	script := fmt.Sprintf(`#!/bin/sh
set -eu
install -d -m 0700 /root/.ssh
key="$(printf '%%s' '%s' | base64 -d)"
touch /root/.ssh/authorized_keys
chmod 0600 /root/.ssh/authorized_keys
grep -Fqx -- "$key" /root/.ssh/authorized_keys || printf '%%s\n' "$key" >> /root/.ssh/authorized_keys
`, encodedKey)
	if _, err := part.Write([]byte(script)); err != nil {
		return "", fmt.Errorf("write authorized-key MIME part: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("close cloud-init MIME message: %w", err)
	}

	return "MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"" + boundary + "\"\r\n\r\n" + out.String(), nil
}
