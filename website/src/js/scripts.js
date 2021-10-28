//dictionary that stores value of soure and destination for copy, sync, and remove
var sourceAndDestination = {
    BlobtoBlob: " 'https://[account name].blob.core.windows.net/[container name]?[SAS key]' 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'",
    BlobtoLocal: " 'https://[account name].blob.core.windows.net/[container name]?[SAS key]' 'C:\\local\\path'",
    BlobtoFiles: " 'https://[account name].blob.core.windows.net/[container name]?[SAS key]' 'https://[account name].file.core.windows.net/[container name]?[SAS key]'",
    FilestoLocal: " 'https://[account name].file.core.windows.net/[container name]?[SAS key]' 'C:\\local\\path'",
    AdlstoLocal: " 'https://[account name].dfs.core.windows.net/[container name]?[SAS key]' 'C:\\local\\path'", 
    AWStoBlob: " 'https://[account name].s3.us-west-2.amazonaws.com/[path]' 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'",
    FilestoFiles: " 'https://[account name].file.core.windows.net/[container name]?[SAS key]' 'https://[account name].file.core.windows.net/[container name]?[SAS key]'",
    LocaltoBlob: " 'C:\\local\\path' 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'",
    FilestoBlob: " 'https://[account name].file.core.windows.net/[container name]?[SAS key]' 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'",
    LocaltoFiles: " 'C:\\local\\path' 'https://[account name].file.core.windows.net/[container name]?[SAS key]'",
    LocaltoAdls: " 'C:\\local\\path' 'https://[account name].dfs.core.windows.net/[container name]?[SAS key]'", 
    GCStoBlob: " 'https://storage.cloud.google.com/[bucket*name]/' 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'", //gcs MISSING
    BlobsRemove: " 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'",
    FilesRemove: " 'https://[account name].file.core.windows.net/[container name]?[SAS key]'",
    AdlsRemove: " 'https://[account name].dfs.core.windows.net/[container name]?[SAS key]'",
    BlobtoBlobSync: " 'https://[account name].blob.core.windows.net/[container name]?[SAS key]' 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'",
    FilestoFilesSync: " 'https://[account name].file.core.windows.net/[container name]?[SAS key]' 'https://[account name].file.core.windows.net/[container name]?[SAS key]'",
    LocaltoBlobSync: " 'C:\\local\\path' 'https://[account name].blob.core.windows.net/[container name]?[SAS key]'",
    BlobtoLocalSync: " 'https://[account name].blob.core.windows.net/[container name]?[SAS key]' 'C:\\local\\path'",
    LocaltoFilesSync: " 'C:\\local\\path' 'https://[account name].file.core.windows.net/[container name]?[SAS key]'",
    BlobtoFilesSync: " 'https://[account name].blob.core.windows.net/[container name]?[SAS key]' 'https://[account name].file.core.windows.net/[container name]?[SAS key]'",
    AdlstoAdls: " 'https://[account name].dfs.core.windows.net/[container name]?[SAS key]' 'https://[account name].dfs.core.windows.net/[container name]?[SAS key]'"
};

//dictionary that store all flag values 
var flagValue = {
    recursive: " --recursive",
    includePattern: " --include-pattern=[patterns]",
    excludePattern: " --exclude-pattern=[patterns]",
    includePath: " --include-path=[pattern]",
    excludePath: " --exclude-path=[patterns]",
    includeRegex: " --include-regex=[patterns]",
    excludeRegex: " --exclude-regex=[patterns]",
    outputType: " --output-type=[text;json]",
    fromTo: " --from-to=[LocalBlob;BlobLocal;LocalBlobFS]",
    dryrun: " --dryrun",
    overwrite: " --overwrite=[true;false;prompt;ifSourceNewer]",
    blobType: " --blob-type=[Detect;BlockBlob;PageBlob;AppendBlob]",
    blobTags: " --blob-tags=[tags]",
    listOfVersions: " --list-of-versions=[version ids]",
    metadata: " --metadata=[metadata]",
    cacheControl: " --cache-control=[cache control]",
    listOfFiles: " --list-of-files=[files]",
    deleteSnapshots: " --delete-snapshots=[string]",
    forceIfReadOnly: " --force-if-read-only",
    logLevel: " --log-level=[info;warning;error;none]",
    deleteDestination: " --delete-destination=[true;false;prompt]",
    mirrorMode: " --mirror-mode",
    preserveInfo: " --preserve-smb-info",
    preservePermission: " --preserve-smb-permissions",
    blockSize: " --block-size-mb=[float]",
    checkMdSync: " --check-md5=[NoCheck;LogOnly;FailIfDifferent;FailIfDifferentOrMissing]",
    putMdSync: " --put-md5"
};

//for copy, remove, sync buttons
function choseCommand(btn) {
    if (btn.id == "copyButton") {
        document.getElementById(btn.id).disabled = true;
        document.getElementById("removeButton").disabled = false;
        document.getElementById("syncButton").disabled = false;

        document.getElementById("copyContainer").style.display="block";
        document.getElementById("removeContainer").style.display="none";
        document.getElementById("syncContainer").style.display="none";
        document.getElementById("commandHolder").style.display="none";

        document.getElementById("message").value="azcopy copy";
    }

    if (btn.id == "removeButton") {
        document.getElementById(btn.id).disabled = true;
        document.getElementById("copyButton").disabled = false;
        document.getElementById("syncButton").disabled = false;

        document.getElementById("copyContainer").style.display="none";
        document.getElementById("removeContainer").style.display="block";
        document.getElementById("syncContainer").style.display="none";
        document.getElementById("commandHolder").style.display="none";
        
        document.getElementById("message").value="azcopy remove";
    }

    if (btn.id == "syncButton") {
        document.getElementById(btn.id).disabled = true;
        document.getElementById("copyButton").disabled = false;
        document.getElementById("removeButton").disabled = false;

        document.getElementById("syncContainer").style.display="block";
        document.getElementById("copyContainer").style.display="none";
        document.getElementById("removeContainer").style.display="none";
        document.getElementById("commandHolder").style.display="none";

        document.getElementById("message").value="azcopy sync";
    } 

    Object.keys(sourceAndDestination).forEach( button => document.getElementById(button).disabled = false);

    document.getElementById("copyFlags").style.display="none";
    document.getElementById("removeFlags").style.display="none";
    document.getElementById("syncFlags").style.display="none";
    document.getElementById("headerFlags").style.display="none";
    document.getElementById("commandHolderTwo").style.display="block";
}


function choseSourceandDestination(btn) {
    //enable all the button 
    Object.keys(sourceAndDestination).forEach( button => document.getElementById(button).disabled = false);
    document.getElementById(btn.id).disabled = true;
    document.getElementById("commandHolderTwo").style.display="none";
    
    //message in text deciding if copy, sync, or remove
    if ((btn.id).includes("Sync")) {
        document.getElementById("syncFlags").style.display="flex";
        document.getElementById("headerFlags").style.display="block";
        Object.keys(flagValue).forEach( flag => {
            if(document.getElementById(flag+"Sync") != null) {
                document.getElementById(flag+"Sync").checked = false;
            }
        });
        document.getElementById("message").value = "azcopy sync" + sourceAndDestination[btn.id];

    } else if ((btn.id).includes("Remove")) {
        document.getElementById("removeFlags").style.display="flex";
        document.getElementById("headerFlags").style.display="block";
        Object.keys(flagValue).forEach( flag => {
            if(document.getElementById(flag+"Remove") != null) {
                document.getElementById(flag+"Remove").checked = false;
            }
        });
        document.getElementById("message").value = "azcopy remove" + sourceAndDestination[btn.id];

    } else {
        document.getElementById("copyFlags").style.display="flex";
        document.getElementById("headerFlags").style.display="block";
        Object.keys(flagValue).forEach( flag => {
            if(document.getElementById(flag+"Copy") != null) {
                document.getElementById(flag+"Copy").checked = false;
            }
        });
        document.getElementById("message").value = "azcopy copy" + sourceAndDestination[btn.id];
    } 
}

function flags(flag, command) {
    //print and removes flag from message box
    dictionaryKey = flag.id.replace(command, '');
    if (document.getElementById(flag.id).checked == true) {
        document.getElementById("message").value += flagValue[dictionaryKey];
    } else {
       var newMessage = document.getElementById("message").value.replace(flagValue[dictionaryKey], '');
       document.getElementById("message").value = newMessage;
    }
}

function copy() {
    let textarea = document.getElementById("message");
    textarea.select();
    document.execCommand("copy");
  }
