## Proposal for merging Avere-CLFSLoad into AzCopy V10 (Phase 1)

### Objective
Implement an AzCopy wrapper over CLFSLoad:
  - Provide the same user experience as the other AzCopy commands.
    - Same style of syntax.
    - Same level of documentation (help messages) for the arguments and flags.

### Current syntax:
```bash
CLFSLoad.py local_state_path source_root target_storage_account target_container sas_token --new
```
 
### Proposed syntax:
```bash
azcopy load /local/source/root "https://[account].blob.core.windows.net/[container]?[SAS]" --state-path="/local/path" --new-session=true
``` 
Reasons:
- AzCopy's convention is to take in only two arguments (source and destination), as too many positional arguments can be error-prone.
    - We can easily parse out the account name, the container name, and the SAS token from the given URL and pass them to CLFSLoad.py.
    - Tools like Storage Explorer (or portal) readily provide the full URL when generating a SAS token, so it's much easier for the user to simply copy/paste it over.
- The command name is `load`, as it resonates with the original name CLFSLoad, and more importantly the meaning behind the operation (to load local files into a CLFS backed by Blob, in a format very different from the source).
    - Creating a new command also makes it easier to document and distinguish it from a regular copy. 
- The `new` flag is renamed slightly for clarify. By default, it should be true.
    - In other words, a new "job" is created by default; the user can choose to continue a previous session by specifying `--new-session=false`.
    - This is not consistent with how the `resume` command works in AzCopy, but since the job tracking is done differently in CLFSLoad, we may be forced to keep it this way for the moment.

### Implementation
1. Package CLFSLoad as an executable:
    - In order to provide a great user experience, we need to package CLFSLoad into an executable using tools such as [PyInstaller](https://www.pyinstaller.org).
        - The current distribution method of asking the user to clone the repo, install Python, create a virtual environment, and running the setup.py works great when CLFSLoad is a standalone project, but it would create too much friction if it was to be embedded in AzCopy. As such, we must be able to distribute CLFSLoad more easily so that there's no installation step before invoking `azcopy load`.
        - AzCopy should remember the hash of the CLFSLoad executable that it shipped with. This way we can verify it and make sure we are invoking the right tool, since we are passing around a form of credential (the SAS).
2. Translate the arguments and flags:
    - This is the easy part.
3. Invoke CLFSLoad and show results:
    - Verify hash first.
    - The rest should be straightforward. Ex: `exec.Command("CLFSLoad.py").Run()`
    - Connect the stdout of CLFSLoad so that the same output is shown to the user. 
        - If time permits, we can format the output of CLFSLoad to be more similar to the other AzCopy commands.

### Other improvements for CLFSLoad:
1.	azure-storage==0.36.0 has been deprecated for quite a while, CLFSLoad should upgrade to azure-storage-blob==2.1.0
2.	azure-mgmt-storage is present in requirements.txt, but it doesn't appear to be used (please correct if wrong). `requirements.txt` should perhaps be scrubbed to make sure only the essential dependencies are packaged/shipped.
