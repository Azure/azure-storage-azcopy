package ste

import (
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

type udamTestSuite struct{}

var _ = chk.Suite(&udamTestSuite{})

func (s *udamTestSuite) MakeUDAMInstance(dummyInstance bool) userDelegationAuthenticationManager {
	if dummyInstance {
		return userDelegationAuthenticationManager{}
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
			credential: udc,
			startTime:  startTime,
			expiryTime: expiryTime,
		}

		udam.sasMap.Store(make(map[string]string))

		return udam
	}
}

func (s *udamTestSuite) TestSASWriteLock(c *chk.C) {
	// We just want a non-empty UDK
	udam := s.MakeUDAMInstance(false)
	var testwg sync.WaitGroup
	var knownSAS string

	// test that the write lock actually works when we're getting a currently unknown container name.
	isLocked := true
	udam.sasMapWriteMutex.Lock()

	testwg.Add(1)
	go func() {
		sas, err := udam.createUserDelegationSASForURL("dummyContainer")
		c.Assert(err, chk.IsNil)
		c.Assert(sas, chk.Not(chk.Equals), "")
		c.Assert(isLocked, chk.Equals, false)
		knownSAS = sas // stash the SAS because we're going to test that in a bit
		c.Log(sas)
		testwg.Done() // ensure that this test actually finishes before we move on
	}()

	time.Sleep(time.Second)

	// unlock the mutex and see the effects
	isLocked = false
	udam.sasMapWriteMutex.Unlock()
	testwg.Wait()

	// test that getting a known container SAS works while the lock is present
	isLocked = true
	udam.sasMapWriteMutex.Lock()

	// Create a dummy URL parts with the known SAS-- only one field matters here
	burlparts := azblob.BlobURLParts{ContainerName: "dummyContainer"}

	testwg.Add(1)
	go func() {
		// this should trigger before we unlock because it is already known
		sas, err := udam.GetUserDelegationSASForURL(burlparts)
		c.Assert(err, chk.IsNil)
		c.Assert(sas, chk.Equals, knownSAS)
		c.Assert(isLocked, chk.Equals, true)
		testwg.Done()
	}()

	time.Sleep(time.Second)

	// unlock the mutex and see the effects
	isLocked = false
	udam.sasMapWriteMutex.Unlock()
	testwg.Wait()
}
