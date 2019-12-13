package ste

import (
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
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
	// sasCache makes use of the thread-safe common.LFUCache.
	// Yes, some routines will inevitably step over eachother.
	// But the stringtosign and such should be the exact same until a refresh.
	sasCache *common.LFUCache
}

// newUserDelegationAuthenticationManager uses an azblob service URL to obtain a user delegation credential, and returns a new userDelegationAuthenticationManager
// serviceURL should have adequate permissions to generate a set of user delegation credentials.
func newUserDelegationAuthenticationManager(serviceURL azblob.ServiceURL) (userDelegationAuthenticationManager, error) {
	authManager := userDelegationAuthenticationManager{serviceURL: serviceURL}
	cache := common.NewLFUCache(100000)
	authManager.sasCache = &cache

	err := authManager.refreshUDKInternal()

	if err != nil {
		return userDelegationAuthenticationManager{}, err
	}

	// Start our refresh loop
	go func() {
		for {
			<-time.After((time.Hour * 24 * 6) + (time.Hour * 12)) // Refresh after exactly 6.5 days so we have .5 days of wiggle room if any SAS tokens are still in use.
			_ = authManager.refreshUDKInternal()                  // We don't need to worry about a retry here.
		}
	}()

	return authManager, nil
}

func (u *userDelegationAuthenticationManager) refreshUDKInternal() error {
	u.sasCache.Range(func(k, v interface{}) bool { // empty the map
		u.sasCache.Delete(k)
		return true
	})

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

	// Check if the SAS token is already present. No need to waste time locking the write mutex if it already exists.
	if sas, ok := u.sasCache.Get(blobURLParts.ContainerName); ok {
		return sas.(string), nil
	} else {
		// if it is not already present, it should be.
		// so, create it and return whatever it returns.
		sas, err := u.createUserDelegationSASForURL(blobURLParts.ContainerName)
		return sas, err
	}
}

func (u *userDelegationAuthenticationManager) createUserDelegationSASForURL(containerName string) (string, error) {
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
	u.sasCache.Set(containerName, sasQuery.Encode())

	if JobsAdmin != nil {
		JobsAdmin.LogToJobLog("Successfully generated SAS token for source container " + containerName)
	}

	// return the SAS query
	return sasQuery.Encode(), nil
}
