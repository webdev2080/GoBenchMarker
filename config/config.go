package config

import (
	"fmt"

	"github.com/oracle/oci-go-sdk/v65/common"
)

// LoadOCIConfig loads the OCI configuration from the specified config file path
func LoadOCIConfig(configFilePath string) (common.ConfigurationProvider, error) {
	fmt.Printf("\nLoading OCI config from: %s\n", configFilePath)
	provider, err := common.ConfigurationProviderFromFile(configFilePath, "DEFAULT")
	if err != nil {
		return nil, fmt.Errorf("failed to load config from file: %v", err)
	}
	return provider, nil
}
