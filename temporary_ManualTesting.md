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


## Sync smb -> file
1. simple sync without changes
2. Sync with changes in the source (added / delted files, added folders)
3. Sync with changes in the target (added / deleted files or folders)
For 2 and 3 test with the flags:

	a. --cfd-mode=TargetCompare
	b. --preserve-smb-info=true
	c. --delete-destination=true
	d. --recursive=true
	e. --preserve-permissions=true
4. Create relative symlink (using windows CMD tool mklink), and play with the permissions using icacls:

		mklink /D <symlink file or director> <relative path to target>
		icalcls <symlink name> /grant <user>:(<permission such: R, M, F>) /L

   Test sync using:

		--follow-symlinks


## Copy smb -> file
simple copy from smb -> files with the below flags:

		a. --recursive=true
		b. --preserve-smb-info=true
		c. --preserve-smb-permissionss=true
		d. --follow-symlinks

## Comp file -> local \ file
simple copy with --recursive=true flag
