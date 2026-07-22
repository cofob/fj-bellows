package digitalocean

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"strings"
)

type userDataPart struct {
	contentType string
	body        string
}

// renderUserData preserves the orchestrator's cloud-config and adds an
// idempotent shell part that installs its SSH key for root. DigitalOcean
// account SSH key IDs are optional and cannot be relied on for every tier.
func renderUserData(cloudConfig, authorizedKey string) (string, error) {
	authorizedKey = strings.TrimSpace(authorizedKey)
	if authorizedKey == "" {
		return validateUserDataSize(cloudConfig)
	}
	if strings.ContainsAny(authorizedKey, "\r\n") {
		return "", errors.New("authorized key must be a single line")
	}

	encodedKey := base64.StdEncoding.EncodeToString([]byte(authorizedKey))
	installKey := fmt.Sprintf(`#!/bin/sh
set -eu
install -d -m 0700 /root/.ssh
key="$(printf '%%s' '%s' | base64 -d)"
touch /root/.ssh/authorized_keys
chmod 0600 /root/.ssh/authorized_keys
grep -Fqx -- "$key" /root/.ssh/authorized_keys || printf '%%s\n' "$key" >> /root/.ssh/authorized_keys
`, encodedKey)

	sum := sha256.Sum256([]byte(cloudConfig + "\x00" + authorizedKey))
	boundary := fmt.Sprintf("fjb-do-%x", sum[:16])
	rendered, err := multipartUserData(boundary, []userDataPart{
		{contentType: `text/cloud-config; charset="us-ascii"`, body: cloudConfig},
		{contentType: `text/x-shellscript; charset="us-ascii"`, body: installKey},
	})
	if err != nil {
		return "", err
	}
	return validateUserDataSize(rendered)
}

func multipartUserData(boundary string, parts []userDataPart) (string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.SetBoundary(boundary); err != nil {
		return "", fmt.Errorf("set MIME boundary: %w", err)
	}
	for _, item := range parts {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", item.contentType)
		part, err := writer.CreatePart(header)
		if err != nil {
			return "", fmt.Errorf("create %s MIME part: %w", item.contentType, err)
		}
		if _, err := part.Write([]byte(item.body)); err != nil {
			return "", fmt.Errorf("write %s MIME part: %w", item.contentType, err)
		}
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close MIME message: %w", err)
	}
	header := "MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"" + boundary + "\"\r\n\r\n"
	return header + body.String(), nil
}

func validateUserDataSize(value string) (string, error) {
	if len(value) > maxUserDataBytes {
		return "", fmt.Errorf("user-data is %d bytes; maximum is %d", len(value), maxUserDataBytes)
	}
	return value, nil
}
