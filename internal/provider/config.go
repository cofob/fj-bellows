package provider

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"
)

// DecodeConfig strictly decodes an opaque provider config node. Provider
// implementations use this instead of yaml.Node.Decode so misspelled fields
// fail startup rather than silently selecting zero values or defaults.
func DecodeConfig(node yaml.Node, out any) error {
	raw, err := yaml.Marshal(&node)
	if err != nil {
		return fmt.Errorf("encode provider config node: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("decode provider config node: %w", err)
	}
	return nil
}
