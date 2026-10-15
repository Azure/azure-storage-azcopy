package azcopy

// ProbeAzureVM reports whether local firmware identifies an Azure public-cloud
// VM. A false result means not detected, not proof that the host is not Azure.
func ProbeAzureVM() bool {
	return probeAzureVM()
}
