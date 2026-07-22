package hetzner

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	labelManaged           = "fjb-managed"
	labelRole              = "fjb-role"
	labelTagHash           = "fjb-tag-hash"
	labelTagParts          = "fjb-tag-parts"
	labelTagPrefix         = "fjb-tag-"
	labelFingerprintParts  = "fjb-fingerprint-parts"
	labelFingerprintPrefix = "fjb-fingerprint-"
	labelManagedValue      = "true"

	roleWorker  = "worker"
	roleBuilder = "builder"
	roleImage   = "image"

	labelChunkChars = 60 // Hetzner label values are limited to 63 characters.
	maxLabelParts   = 16
)

var rawBase32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func tagHash(tag string) string {
	sum := sha256.Sum256([]byte(tag))
	return strings.ToLower(rawBase32.EncodeToString(sum[:]))
}

func ownershipSelector(tag, role string) string {
	return fmt.Sprintf("%s=true,%s=%s,%s=%s", labelManaged, labelRole, role, labelTagHash, tagHash(tag))
}

func ownershipLabels(tag, role string) (map[string]string, error) {
	if strings.TrimSpace(tag) == "" {
		return nil, errors.New("ownership tag must not be empty")
	}
	labels := map[string]string{
		labelManaged: labelManagedValue,
		labelRole:    role,
		labelTagHash: tagHash(tag),
	}
	if err := encodeLabelChunks(labels, labelTagParts, labelTagPrefix, tag); err != nil {
		return nil, fmt.Errorf("encode ownership tag: %w", err)
	}
	return labels, nil
}

func snapshotLabels(tag, fingerprint string) (map[string]string, error) {
	labels, err := ownershipLabels(tag, roleImage)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(fingerprint) == "" {
		return nil, errors.New("fingerprint must not be empty")
	}
	if err := encodeLabelChunks(labels, labelFingerprintParts, labelFingerprintPrefix, fingerprint); err != nil {
		return nil, fmt.Errorf("encode fingerprint: %w", err)
	}
	return labels, nil
}

func encodeLabelChunks(labels map[string]string, countKey, prefix, value string) error {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(value))
	parts := (len(encoded) + labelChunkChars - 1) / labelChunkChars
	if parts == 0 {
		parts = 1
	}
	if parts > maxLabelParts {
		return fmt.Errorf("value requires %d label parts; maximum is %d", parts, maxLabelParts)
	}
	labels[countKey] = strconv.Itoa(parts)
	for i := range parts {
		start := i * labelChunkChars
		end := min(start+labelChunkChars, len(encoded))
		labels[prefix+strconv.Itoa(i)] = encoded[start:end]
	}
	return nil
}

func decodeLabelChunks(labels map[string]string, countKey, prefix string) (string, error) {
	parts, err := strconv.Atoi(labels[countKey])
	if err != nil || parts <= 0 || parts > maxLabelParts {
		return "", fmt.Errorf("invalid %s label %q", countKey, labels[countKey])
	}
	var encoded strings.Builder
	for i := range parts {
		part, ok := labels[prefix+strconv.Itoa(i)]
		if !ok {
			return "", fmt.Errorf("missing %s%d label", prefix, i)
		}
		encoded.WriteString(part)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded.String())
	if err != nil {
		return "", fmt.Errorf("decode %s labels: %w", prefix, err)
	}
	return string(decoded), nil
}

func hasOwnership(labels map[string]string, tag, role string) bool {
	if labels[labelManaged] != labelManagedValue || labels[labelRole] != role || labels[labelTagHash] != tagHash(tag) {
		return false
	}
	decoded, err := decodeLabelChunks(labels, labelTagParts, labelTagPrefix)
	return err == nil && decoded == tag
}

func sameOwnership(a, b map[string]string) bool {
	return a[labelManaged] == labelManagedValue && b[labelManaged] == labelManagedValue &&
		a[labelTagHash] != "" && a[labelTagHash] == b[labelTagHash]
}
