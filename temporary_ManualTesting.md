# Manual Testing
`This is temporary file - to test our changes manually until we will stable the unit/e2e tests`


## Copy simple scenrio
1. Test copy from nfs/blob
2. Test copy from smb/file


## Sync nfs -> blob
1. Simple sync without changes
2. Rename source dierctory and sync after with all of the options (one at a time):

		a. --cfd-mode=CtimeMtime (--last-sync-time=YYYY-MM-DDTHH:MM:SS.0000000Z)
		b. --cfd-mode=Ctime (--last-sync-time=YYYY-MM-DDTHH:MM:SS.0000000Z)
		c. --cfd-mode=TargetCompare  //Currently broken
		d. --metadata-only-sync=true

3. Delete source file and sync with the flag:

		 --mirror-mode //Currently broken


## sync smb -> file
1. simple sync without changes