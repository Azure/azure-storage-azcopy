package ste

import (
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/golang/groupcache/lru"
	chk "gopkg.in/check.v1"
)

type udamTestSuite struct{}

var _ = chk.Suite(&udamTestSuite{})

// MakeUDAMInstance uses fake values in order to create a mostly-dummy instance of UDAM that works, but doesn't produce working tokens.
func (s *udamTestSuite) MakeUDAMInstance(dummyInstance bool) userDelegationAuthenticationManager {
	cache := lru.New(10)
	if dummyInstance {
		return userDelegationAuthenticationManager{
			credInfoLock: &sync.Mutex{},
			sasCache:     cache,
		}
	} else {
		startTime := time.Now()
		expiryTime := time.Now().Add(time.Hour * 24)

		// this credential and key isn't real, obviously.
		udc := azblob.NewUserDelegationCredential("dummyAccount", azblob.UserDelegationKey{
			SignedOid:     "dummyoid",
			SignedTid:     "dummytid",
			SignedStart:   startTime,
			SignedExpiry:  expiryTime,
			SignedService: "b",
			SignedVersion: DefaultServiceApiVersion,
			Value:         "/mzvUcYFlEGeSUSOT6AShSzbruPBueLN2E/1hJ1HV9M=",
		})

		// create a actual working instance of UDAM, but don't use the normal creation path
		udam := userDelegationAuthenticationManager{
			credInfoLock: &sync.Mutex{},
			credential:   udc,
			startTime:    startTime,
			expiryTime:   expiryTime,
			sasCache:     cache,
		}

		return udam
	}
}

func (s *udamTestSuite) TestGetSASToken(c *chk.C) {
	// We just want a non-empty UDK
	udam := s.MakeUDAMInstance(false)
	var knownSAS string

	// Get an already existing SAS
	knownSAS, err := udam.createUserDelegationSASForURL("dummyContainer")
	c.Assert(err, chk.IsNil)

	o, err := udam.GetUserDelegationSASForURL(azblob.BlobURLParts{ContainerName: "dummyContainer"})
	c.Assert(err, chk.IsNil)
	c.Assert(o, chk.Equals, knownSAS)

	// Try getting a new SAS
	newSAS, err := udam.GetUserDelegationSASForURL(azblob.BlobURLParts{ContainerName: ""})
	c.Assert(err, chk.IsNil)
	c.Assert(newSAS, chk.Not(chk.Equals), "")
}