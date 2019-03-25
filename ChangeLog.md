# Change Log

## Version 10.0.8:

- Rewrote sync command to eliminate numerous bugs and improve usability (see wiki for details)
- Implemented various improvements to memory management
- Added MD5 validation support (available options: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing)
- Added last modified time checks for source to guarantee transfer integrity 
- Formalized outputs in JSON and elevated the output flag to the root level
- Eliminated outputs to STDERR (for new version notifications), which were causing problems for certain CI systems
- Improved log format for Windows
- Optimized plan file sizes
- Improved command line parameter names as follows (to be consistent with naming pattern of other parameters):
  - fromTo -> from-to
  - blobType -> blob-type
  - excludedBlobType -> excluded-blob-type
  - outputRaw (in "list" command) -> output
  - stdIn-enable (reserved for internal use) -> stdin-enable

## Version 10.0.9:

- For creating MD5 hashes when uploading, version 10.x now has the OPPOSITE default to version AzCopy 8.x. Specifically, in version 10.x, MD5 hashes of content are now only created if you specify `--put-md5` on the command line