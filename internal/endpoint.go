package internal

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

func ParseEndpoint(endpoint string) (string, func(), error) {
	if !strings.HasPrefix(endpoint, "unix://") {
		return "", nil, fmt.Errorf("only unix:// endpoints are supported, got %q", endpoint)
	}
	address := strings.TrimPrefix(endpoint, "unix://")
	if address == "" {
		return "", nil, errors.New("empty unix socket path")
	}
	cleanup := func() {
		_ = os.Remove(address)
	}
	_ = os.Remove(address)
	return address, cleanup, nil
}
