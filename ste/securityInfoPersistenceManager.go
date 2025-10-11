package ste

import (
	"context"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"

	"github.com/golang/groupcache/lru"
)

// securityInfoPersistenceManager implements a system to interface with Azure Files
// (since this is the only remote at the moment that is SDDL aware)
// in which SDDL strings can be uploaded and mapped to their remote IDs, then obtained from their remote IDs.
type securityInfoPersistenceManager struct {
	sipmMu *sync.RWMutex
	cache  *lru.Cache
	ctx    context.Context
}

// Files supports SDDLs up to and equal to 8kb. Because this isn't KiB, We're going to infer that it's 8x1000, not 8x1024.
var FilesServiceMaxSDDLSize = 8000

func newSecurityInfoPersistenceManager(ctx context.Context) *securityInfoPersistenceManager {
	return &securityInfoPersistenceManager{
		sipmMu: &sync.RWMutex{},
		cache:  lru.New(3000), // Assuming all entries are around 9kb, this would use around 30MB.
		ctx:    ctx,
	}
}

// Technically, yes, GetSDDLFromID can be used in conjunction with PutSDDL.
// Being realistic though, GetSDDLFromID will only be called when downloading,
// and PutSDDL will only be called when uploading/doing S2S.
func (sipm *securityInfoPersistenceManager) PutSDDL(sddlString string, shareClient *share.Client) (string, error) {
	sddlKey := shareClient.URL() + "|SDDL|" + sddlString

	// Acquire a read lock.
	sipm.sipmMu.RLock()
	// First, let's check the cache for a hit or miss.
	// These IDs are per share, so we use a share-unique key.
	// The SDDL string will be consistent from a local source.
	id, ok := sipm.cache.Get(sddlKey)
	sipm.sipmMu.RUnlock()

	if ok {
		return id.(string), nil
	}

	cResp, err := shareClient.CreatePermission(sipm.ctx, sddlString, nil)

	if err != nil {
		return "", err
	}

	permKey := *cResp.FilePermissionKey

	sipm.sipmMu.Lock()
	sipm.cache.Add(sddlKey, permKey)
	sipm.sipmMu.Unlock()

	return permKey, nil
}

func (sipm *securityInfoPersistenceManager) GetSDDLFromID(id string, shareClient *share.Client) (string, error) {

	//GetPermission call only works against the share root, and not against a share snapshot
	sddlKey := shareClient.URL() + "|ID|" + id

	sipm.sipmMu.Lock()
	// fetch from the cache
	// The SDDL string will be consistent from a local source.
	perm, ok := sipm.cache.Get(sddlKey)
	sipm.sipmMu.Unlock()

	if ok {
		return perm.(string), nil
	}

	//remove snap if any
	shareClient, err := shareClient.WithSnapshot("")
	if err != nil {
		return "", err
	}
	si, err := shareClient.GetPermission(sipm.ctx, id, nil)
	if err != nil {
		return "", err
	}

	sipm.sipmMu.Lock()
	// If we got the permission fine, commit to the cache.
	sipm.cache.Add(sddlKey, *si.Permission)
	sipm.sipmMu.Unlock()

	return *si.Permission, nil
}
