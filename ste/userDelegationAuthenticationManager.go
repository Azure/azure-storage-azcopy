package ste

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// userDelegationAuthenticationManager manages the automatic creation of user delegation SAS tokens for each container.
// UDAM is only triggered when
// 1) The user performs a service-to-service transfer
// and
// 2) The user did not supply a source SAS token on a blob transfer.
//
// When UDAM spins up, a credential is created.
// UDAM is not necessary in the frontend because if UDAM would succeed, the OAuth token would have to be capable of enumerating the container to start with.
type userDelegationAuthenticationManager struct {
	// Because User Delegation SAS tokens cannot last longer than a user delegation credential, we must hold onto the key info.
	startTime  time.Time
	expiryTime time.Time
	credential azblob.UserDelegationCredential
	serviceURL azblob.ServiceURL
	// This is a mostly-read scenario. Therefore, we treat the SAS map as an atomic value, and apply a write mutex to the map.
	// We then, whenever we're attempting to get a SAS token for a container, check if it's present first.
	// If it's not present, we start attempting to create the token. In order to do so, we lock the write mutex.
	// Since this may take time, and by the time we obtain the lock, the SAS we're trying to get MAY have already been created,
	// Check the map for the key before we overwrite.
	sasMap           *atomic.Value // points to a map[string]string. map[containerName]sasToken
	sasMapWriteMutex *sync.Mutex
}

// newUserDelegationAuthenticationManager uses an azblob service URL to obtain a user delegation credential, and returns a new userDelegationAuthenticationManager
// serviceURL should have adequate permissions to generate a set of user delegation credentials.
func newUserDelegationAuthenticationManager(serviceURL azblob.ServiceURL) (userDelegationAuthenticationManager, error) {
	authManager := userDelegationAuthenticationManager{serviceURL: serviceURL, sasMap: &atomic.Value{}, sasMapWriteMutex: &sync.Mutex{}}

	err := authManager.refreshUDKInternal()

	if err != nil {
		return userDelegationAuthenticationManager{}, err
	}

	// Start our refresh loop
	go func() {
		for {
			<-time.After((time.Hour * 24 * 6) + (time.Hour * 12))
			_ = authManager.refreshUDKInternal() // We don't need to worry about a retry here.
		}
	}()

	return authManager, nil
}

func (u *userDelegationAuthenticationManager) refreshUDKInternal() error {
	// First, grab the SAS token map lock. We need to flush it.
	u.sasMapWriteMutex.Lock()
	defer u.sasMapWriteMutex.Unlock()
	// Flush the SAS token map-- Everything in there is now invalid.
	u.sasMap.Store(make(map[string]string))

	// Create a new start/expiry time.
	u.startTime = time.Now()
	u.expiryTime = u.startTime.Add(time.Hour * 24 * 7)
	keyInfo := azblob.NewKeyInfo(u.startTime, u.expiryTime)

	if JobsAdmin != nil {
		JobsAdmin.LogToJobLog("Attempting to refresh the User Delegation Credential for the blob source.")
	}

	var err error
	u.credential, err = u.serviceURL.GetUserDelegationCredential(steCtx, keyInfo, nil, nil)

	if err != nil {
		if JobsAdmin != nil {
			JobsAdmin.LogToJobLog("Failed to obtain a User Delegation Credential for the blob source.")
		}

		// Clear the user delegation credential-- it is now invalid.
		u.credential = azblob.UserDelegationCredential{}
		return err
	}

	if JobsAdmin != nil {
		JobsAdmin.LogToJobLog("Successfully obtained a User Delegation Credential for the blob source.")
	}

	return nil
}

func (u *userDelegationAuthenticationManager) GetUserDelegationSASForURL(blobURLParts azblob.BlobURLParts) (string, error) {
	// Go doesn't seem to parse this quite right without the ().
	if u.credential == (azblob.UserDelegationCredential{}) {
		// If we don't have creds, we can't return a SAS token anyway.
		return "", nil
	}

	sasMap := u.sasMap.Load().(map[string]string)

	// Check if the SAS token is already present. No need to waste time locking the write mutex if it already exists.
	if sas, ok := sasMap[blobURLParts.ContainerName]; ok {
		return sas, nil
	} else {
		// if it is not already present, it should be.
		// so, create it and return whatever it returns.
		sas, err := u.createUserDelegationSASForURL(blobURLParts.ContainerName)
		return sas, err
	}
}

func (u *userDelegationAuthenticationManager) createUserDelegationSASForURL(containerName string) (string, error) {
	// First, obtain the mutex
	u.sasMapWriteMutex.Lock()
	// defer an unlock
	defer u.sasMapWriteMutex.Unlock()

	// check against the SAS map again in case something already created it.
	sasMap := u.sasMap.Load().(map[string]string)

	// If it's already present, just return it.
	// We do this check a second time in a row because something may have stepped over us.
	if sas, ok := sasMap[containerName]; ok {
		return sas, nil
	}

	// If it's not present, we need to generate a SAS query and store it, then return.
	sasQuery, err := azblob.BlobSASSignatureValues{
		Version:       DefaultServiceApiVersion,
		Protocol:      azblob.SASProtocolHTTPSandHTTP, // A user may be inclined to use HTTP for one reason or another. We already warn them about this.
		StartTime:     u.startTime,
		ExpiryTime:    u.expiryTime,
		Permissions:   azblob.ContainerSASPermissions{Read: true, List: true}.String(), // read-only perms, effectively
		ContainerName: containerName,
	}.NewSASQueryParameters(u.credential)

	if err != nil {
		return "", err
	}

	// Write the query to the map and then store it.
	sasMap[containerName] = sasQuery.Encode()
	u.sasMap.Store(sasMap)

	if JobsAdmin != nil {
		JobsAdmin.LogToJobLog("Successfully generated SAS token for source container " + containerName)
	}

	// return the SAS query
	return sasQuery.Encode(), nil
}
