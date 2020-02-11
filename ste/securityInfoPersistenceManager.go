package ste

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/golang/groupcache/lru"

	"github.com/Azure/azure-storage-azcopy/common"
)

// securityInfoPersistenceManager implements a system to interface with Azure Files
// (since this is the only remote at the moment that is SDDL aware)
// in which SDDL strings can be uploaded and mapped to their remote IDs, then obtained from their remote IDs.
type securityInfoPersistenceManager struct {
	sipmMu *sync.Mutex
	cache  *lru.Cache
	ctx    context.Context
	// No particular need for a sync.Map since the entire object is mutexed.
	// NOTE: If user delegation SAS tokens ever get introduced to files, or we ever support a different remote that is SDDL aware,
	//       consider changing the SIPM to an interface where the put/get system is adapted for that src/dst combo
	serviceURLs map[string]azfile.ServiceURL
}

// Files supports SDDLs up to and equal to 8kb. Because this isn't KiB, We're going to infer that it's 8x1000, not 8x1024.
var filesServiceMaxSDDLSize = 8000

func newSecurityInfoPersistenceManager(ctx context.Context) *securityInfoPersistenceManager {
	return &securityInfoPersistenceManager{
		sipmMu:      &sync.Mutex{},
		cache:       lru.New(3000), // Assuming all entries are around 9kb, this would use around 30MB.
		serviceURLs: make(map[string]azfile.ServiceURL),
		ctx:         ctx,
	}
}

func (sipm *securityInfoPersistenceManager) PutServiceURL(serviceURL azfile.ServiceURL) {
	fURLParts := common.NewGenericResourceURLParts(serviceURL.URL(), common.ELocation.File())

	sipm.sipmMu.Lock()
	defer sipm.sipmMu.Unlock()

	sipm.serviceURLs[fURLParts.GetAccountName()] = serviceURL
}

// Technically, yes, GetSDDLFromID can be used in conjunction with PutSDDL.
// Being realistic though, GetSDDLFromID will only be called when downloading,
// and PutSDDL will only be called when uploading/doing S2S.
func (sipm *securityInfoPersistenceManager) PutSDDL(acctName, shareName, sddlString string, p pipeline.Pipeline) (string, error) {
	sipm.sipmMu.Lock()
	defer sipm.sipmMu.Unlock()

	sddlKey := acctName + "|SHARE|" + shareName + "|SDDL|" + sddlString

	// First, let's check the cache for a hit or miss.
	// These IDs are per share, so we use a share-unique key.
	// The SDDL string will be consistent from a local source.
	id, ok := sipm.cache.Get(sddlKey)

	if ok {
		return id.(string), nil
	}

	serviceURL, ok := sipm.serviceURLs[acctName]

	if !ok {
		return "", fmt.Errorf("service URL wasn't found for account %s", acctName)
	}

	serviceURL = serviceURL.WithPipeline(p)

	shareURL := serviceURL.NewShareURL(shareName)

	cResp, err := shareURL.CreatePermission(sipm.ctx, sddlString)

	if err != nil {
		return "", err
	}

	permKey := cResp.FilePermissionKey()

	sipm.cache.Add(sddlKey, permKey)

	return permKey, nil
}

func (sipm *securityInfoPersistenceManager) GetSDDLFromID(acctName, shareName, id string, p pipeline.Pipeline) (string, error) {
	sipm.sipmMu.Lock()
	defer sipm.sipmMu.Unlock()

	sddlKey := acctName + "|SHARE|" + shareName + "|ID|" + id

	// fetch from the cache
	// The SDDL string will be consistent from a local source.
	perm, ok := sipm.cache.Get(sddlKey)

	if ok {
		return perm.(string), nil
	}

	serviceURL, ok := sipm.serviceURLs[acctName]

	if !ok {
		return "", fmt.Errorf("service URL wasn't found for account %s", acctName)
	}

	serviceURL = serviceURL.WithPipeline(p)

	shareURL := serviceURL.NewShareURL(shareName)

	si, err := shareURL.GetPermission(sipm.ctx, id)

	if err != nil {
		return "", err
	}

	// If we got the permission fine, commit to the cache.
	sipm.cache.Add(sddlKey, si.Permission)

	return si.Permission, nil
}
