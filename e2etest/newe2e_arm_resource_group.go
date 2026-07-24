package e2etest

import (
	"net/http"
	"net/url"
	"path"
)

// ========== Client ==========

type ARMResourceGroup struct {
	ParentSubject[*ARMSubscription]
	ResourceGroupName string
}

func (rg *ARMResourceGroup) CanonicalPath() string {
	return path.Join(rg.ParentSubject.CanonicalPath(), "resourcegroups", rg.ResourceGroupName)
}

func (rg *ARMResourceGroup) NewStorageAccountARMClient(acctName string) *ARMStorageAccount {
	return &ARMStorageAccount{
		ParentSubject: ParentSubject[*ARMResourceGroup]{
			parent: rg,
			root:   rg.root,
		},
		AccountName: acctName,
	}
}

func (rg *ARMResourceGroup) NewManagedDiskARMClient(diskName string) *ARMManagedDisk {
	return &ARMManagedDisk{
		ParentSubject: ParentSubject[*ARMResourceGroup]{
			parent: rg,
			root:   rg.root,
		},
		DiskName: diskName,
	}
}

type ARMResourceGroupCreateParams struct {
	Location   string                `json:"location"` // required
	ManagedBy  *string               `json:"managedBy,omitempty"`
	Properties *ARMResourceGroupInfo `json:"properties,omitempty"`
	Tags       map[string]string     `json:"tags,omitempty"`
}

type ARMResourceGroupProvisioningStateOutput struct {
	ProvisioningState string `json:"provisioningState"`
}

func (rg *ARMResourceGroup) PrepareRequest(reqSettings *ARMRequestSettings) {
	if reqSettings.Query == nil {
		reqSettings.Query = make(url.Values)
	}

	if !reqSettings.Query.Has("api-version") {
		reqSettings.Query.Add("api-version", "2021-04-01") // Attach default query
	}
}

func (rg *ARMResourceGroup) CreateOrUpdate(params ARMResourceGroupCreateParams) (*ARMResourceGroupProvisioningStateOutput, error) {
	var out ARMResourceGroupProvisioningStateOutput
	_, err := PerformRequest(rg, ARMRequestSettings{
		Method: http.MethodPut,
		Body:   params,
	}, &out) // Shouldn't "officially" incur an async operation according to docs, and PrepareRequest should catch an error state on that for us.
	if err != nil {
		return nil, err
	}

	return &out, nil
}

func (rg *ARMResourceGroup) Delete(forceDeletionTypes *string) error {
	var query = make(url.Values)
	if forceDeletionTypes != nil {
		query.Add("forceDeletionTypes", *forceDeletionTypes)
	}

	_, err := PerformRequest[any](rg, ARMRequestSettings{
		Method: http.MethodDelete,
	}, nil) // No need to have a response
	if err != nil {
		return err
	}

	return nil
}

func (rg *ARMResourceGroup) GetProperties() (*ARMResourceGroupInfo, error) {
	var out ARMResourceGroupInfo
	_, err := PerformRequest(rg, ARMRequestSettings{
		Method: http.MethodGet,
	}, &out)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// ========= Shared Structs ==========

type ARMResourceGroupInfo struct {
	ID                    string                                  `json:"id"`
	Location              string                                  `json:"location"`
	ManagedBy             string                                  `json:"managedBy"`
	ProvisioningStateInfo ARMResourceGroupProvisioningStateOutput `json:"properties"`
	Tags                  []string                                `json:"tags"`
	Type                  string                                  `json:"type"`
}
