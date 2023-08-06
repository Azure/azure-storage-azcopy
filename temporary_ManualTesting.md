# Manual Testing
`This is temporary file - to test our changes manually until we will stable the unit/e2e tests`


`In addition to this file - you should also run the e2e tests - to cover the main scenarios`



## Sync nfs -> blob
1. Rename source dierctory and sync after with all of the options (one at a time):

		a. --cfd-mode=CtimeMtime (--last-sync-time=YYYY-MM-DDTHH:MM:SS.0000000Z)
		b. --cfd-mode=Ctime (--last-sync-time=YYYY-MM-DDTHH:MM:SS.0000000Z)
		c. --cfd-mode=TargetCompare  //Currently broken
		d. --metadata-only-sync=true


## Sync smb -> file
1. Sync with changes in the source (added / delted files, added folders)
2. Sync with changes in the target (added / deleted files or folders)
For 1 and 2 test with the flags:

	a. --cfd-mode=TargetCompare
	b. --preserve-smb-info=true
	c. --delete-destination=true
	d. --recursive=true
	e. --preserve-permissions=true
3. Create relative symlink (using windows CMD tool mklink), and play with the permissions using icacls:

		mklink /D <symlink file or director> <relative path to target>
		icalcls <symlink name> /grant <user>:(<permission such: R, M, F>) /L

   Test sync using:

		--follow-symlinks


## Copy smb -> file
simple copy from smb -> files with the below flags:

		a. --preserve-smb-info=true
		b. --preserve-smb-permissionss=true
		c. --follow-symlinks

