package hetzner

import (
	"errors"
	"testing"

	"github.com/hetznercloud/hcloud-go/v2/hcloud"
)

func TestClassifyCreateError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "direct capacity error",
			err:       hcloud.Error{Code: hcloud.ErrorCodeResourceUnavailable, Message: "capacity"},
			retryable: true,
		},
		{
			name:      "asynchronous placement error",
			err:       hcloud.ActionError{Code: string(hcloud.ErrorCodePlacementError), Message: "placement"},
			retryable: true,
		},
		{
			name: "invalid token",
			err:  hcloud.Error{Code: hcloud.ErrorCodeUnauthorized, Message: "unauthorized"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyCreateError(tt.err)
			if errors.Is(got, ErrLocationUnavailable) != tt.retryable {
				t.Fatalf("classifyCreateError(%v) = %v, retryable = %t", tt.err, got, tt.retryable)
			}
			if !errors.Is(got, tt.err) {
				t.Fatalf("classified error %v does not wrap source %v", got, tt.err)
			}
		})
	}
}
