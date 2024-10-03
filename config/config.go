package config

import (
	"fmt"

	"github.com/oracle/oci-go-sdk/v65/common"
)

// LoadOCIConfig loads the OCI configuration from the specified config file path
func LoadOCIConfig(configFilePath string) (common.ConfigurationProvider, error) {
	fmt.Printf("Loading OCI config from: %s\n", configFilePath) // Log the config file being used
	provider, err := common.ConfigurationProviderFromFile(configFilePath, "DEFAULT")
	if err != nil {
		return nil, err
	}

	return provider, nil
}
