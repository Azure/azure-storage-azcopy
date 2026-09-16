function Get-AzCopyFieldDescription([string[]]$Names) {
    $definitions = @{
        SourceType = 'SourceType: Source storage backend, such as Local, Blob, BlobFS (Data Lake Storage), File, FileNFS, S3, or GCP, derived from FromTo. Aggregate Sources lists the observed source backend types.'
        SourceProtocol = 'SourceProtocol: Backend-derived access category: Blob/BlobFS/File = https, FileNFS = nfs, Local = local, S3 = s3, GCP = gcs. This is not observed network traffic; a mounted network path classified as Local still reports local.'
        SourceScope = 'SourceScope: Level of the selected source root: service, container, share, bucket, object-or-prefix, local-directory, local-object, stream, or benchmark. This is not authorization scope or directory depth. The selected root is excluded from source-relative depth.'
        SourceMountType = 'SourceMountType: Source backing-store category: cloud-azure, cloud-s3, cloud-gcs, local-disk, nas-smb, or nas-nfs. Local paths are refined using OS mount information where supported; unavailable mount detection falls back to local-disk.'
        SourceEndpointKind = 'SourceEndpointKind: Azure source hostname heuristic: private-endpoint when the hostname contains .privatelink., otherwise public; empty for non-Azure or unparseable hosts. No DNS or route inspection is performed. A normal hostname resolving privately can still report public. Historical events without this field display unknown, not public. Source and destination are classified independently.'
        SourceCloudType = 'SourceCloudType: Azure environment inferred from the source hostname suffix: public, gov, china, or germany; unknown for unrecognized Azure hosts and empty for non-Azure sources. This is not public/private network classification.'
        SourceAuthMechanism = 'SourceAuthMechanism: Source credential category, not credential material. SAS is reported when a SAS is present, PublicAnonymous for anonymous remote access, NotApplicable for local/stream/benchmark endpoints, otherwise the selected credential-type name (for example OAuthToken). It is not an authorization scope or proof of successful authentication.'
        DestType = 'DestType: Destination storage backend, such as Local, Blob, BlobFS, File, FileNFS, S3, or GCP; derived from the destination half of FromTo.'
        DestProtocol = 'DestProtocol: Backend-derived access category: Blob/BlobFS/File = https, FileNFS = nfs, Local = local, S3 = s3, GCP = gcs. This is not observed network traffic; OS-mounted network paths classified as Local still report local.'
        DestScope = 'DestScope: Resource level inferred from the supplied destination path: service, container, share, bucket, object-or-prefix, local-directory, or local-object (with stream/none/unknown for special cases). This is not an authorization scope; object-or-prefix does not distinguish an individual object from a virtual directory. Local destination classification is syntax-based, not verified filesystem type.'
        DestEndpointKind = 'DestEndpointKind: Azure destination hostname heuristic: private-endpoint when the hostname contains .privatelink., otherwise public; empty for non-Azure or unparseable hosts. No DNS or route inspection is performed, so a normal hostname resolving privately can still report public. This is not the Azure cloud environment.'
        InvocationContext = 'InvocationContext: ci when any of TF_BUILD, GITHUB_ACTIONS, CI, JENKINS_URL, GITLAB_CI, or BUILD_BUILDID is nonempty in the AzCopy process; otherwise interactive. Even CI=false counts as ci. interactive means no recognized CI marker, not proof of a human or terminal session. No SDK or parent-process detection is performed. E2ETestRunID does not affect this field; a controlled child environment can omit CI markers.'
        'azcopy.source_max_directory_depth' = 'azcopy.source_max_directory_depth: Maximum directory nesting of eligible scanned payload objects relative to the selected source root, after traversal filters. file.txt = 0; child/file.txt = 1; a/b/file.txt = 2. The selected root is not counted. Folder-property entries, including empty directories, do not contribute; zero can also mean no eligible payload objects were observed. One root-folder transfer plus one direct file can correctly report zero.'
        'azcopy.hardlinks_converted_scheduled' = 'azcopy.hardlinks_converted_scheduled: Recognized hardlink entries scheduled as independent file copies with --hardlinks=follow (the default), for transfers involving Azure Files NFS. Hardlinks are filenames sharing one underlying file/inode, not symbolic links. Two recognized selected names sharing an inode count as two conversions. preserve and skip do not increment this counter; scheduled does not mean completed. Zero is expected for BlobBlob and does not prove a filesystem contains no hardlinks.'
        'azcopy.storage_http_attempt_count' = 'azcopy.storage_http_attempt_count: Total returned attempts counted by the instrumented Storage HTTP pipeline, including retries and failed attempts. It is a count, not a rate or number of files. Application Insights sends and local disk operations are excluded. Charts that sum this metric count observed attempts, not successful transactions.'
        'azcopy.avg_iops' = 'azcopy.avg_iops: Integer-truncated average instrumented Storage HTTP attempts per second since the pipeline statistics object was initialized. It uses the same counter as storage_http_attempt_count, including retries/failures, but a separate timer whose start loses fractional seconds. It is not disk IOPS or successful files/second. count / job_duration_seconds gives a reproducible job-wide rate, not necessarily this exact value; the timer is not transfer-phase duration. Dashboard averages of job rates are not a fleet-wide rate.'
        'azcopy.bytes_enumerated' = 'azcopy.bytes_enumerated (scheduled source bytes): Sum of source sizes in scheduled job-plan entries after traversal filters and copy/sync selection. Unchanged sync objects are not included; work that later fails or is skipped can remain included. This is not successful-transfer volume. Resume summary counters can be job-cumulative.'
        'azcopy.source_bytes_scanned' = 'azcopy.source_bytes_scanned (source bytes examined after filters): Sum of listed sizes of eligible source payload objects reaching the scan callback after traversal filters, before sync comparison selects transfers. BytesScanned and Source bytes scanned name this same counter, not separate metrics. It includes unchanged sync objects, excludes destination scanning and folder-property operations, and is not payload bytes read or total account size. Symlink/hardlink eligibility follows the configured handling mode.'
    }
    $definitions.TargetType = 'TargetType is the aggregate alias of DestType; Targets lists the observed destination backend types. ' + $definitions.DestType
    $text = foreach ($name in $Names) {
        if (-not $definitions.ContainsKey($name)) { throw "Unknown dashboard field: $name" }
        $definitions[$name]
    }
    return ($text -join "`n`n")
}

function Add-AzCopyVisibleFieldGuides($Dashboard) {
    $fields = @('SourceType', 'SourceProtocol', 'SourceScope', 'SourceMountType', 'SourceEndpointKind', 'SourceCloudType', 'SourceAuthMechanism', 'DestType', 'DestProtocol', 'DestScope', 'DestEndpointKind', 'TargetType', 'InvocationContext',
        'azcopy.bytes_enumerated', 'azcopy.source_bytes_scanned', 'azcopy.source_max_directory_depth',
        'azcopy.hardlinks_converted_scheduled', 'azcopy.storage_http_attempt_count', 'azcopy.avg_iops')
    $isAdx = $Dashboard.Contains('tiles')
    $pages = if ($isAdx) { $Dashboard.pages } else { @(@{ id = $Dashboard.uid }) }
    foreach ($page in $pages) {
        $panels = if ($isAdx) { @($Dashboard.tiles | Where-Object pageId -eq $page.id) } else { @($Dashboard.panels) }
        $parameters = if ($isAdx) {
            @($Dashboard.parameters | Where-Object { -not $_.showOnPages -or $_.showOnPages.kind -eq 'all' -or $page.id -in $_.showOnPages.pageIds })
        } else { @($Dashboard.templating.list) }
        $descriptions = (@($panels.description) + @($parameters.description)) -join "`n"
        $relevant = @($fields | Where-Object { $descriptions.Contains((Get-AzCopyFieldDescription @($_))) })
        if ($relevant.Count -eq 0) { continue }
        $markdown = ($relevant | ForEach-Object { '**' + $_ + '**' + "`n`n" + (Get-AzCopyFieldDescription @($_)) }) -join "`n`n"
        $lines = ($relevant | ForEach-Object { 4 + [Math]::Ceiling((Get-AzCopyFieldDescription @($_)).Length / 85.0) } | Measure-Object -Sum).Sum
        $height = [int][Math]::Max(6, [Math]::Ceiling($lines * 0.8) + 2)
        if ($isAdx) {
            $bottom = ($panels | ForEach-Object { $_.layout.y + $_.layout.height } | Measure-Object -Maximum).Maximum
            $width = ($panels | ForEach-Object { $_.layout.x + $_.layout.width } | Measure-Object -Maximum).Maximum
            $hash = [Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes("azcopy-field-guide:$($page.id)"))
            $id = ([Guid]::new([byte[]]$hash[0..15])).ToString()
            $Dashboard.tiles += [ordered]@{
                id = $id; title = 'Field definitions'; description = ''; pageId = $page.id
                visualType = 'markdownCard'; markdownText = $markdown; visualOptions = [ordered]@{}
                layout = [ordered]@{ x = 0; y = $bottom; width = $width; height = $height }
            }
        } else {
            $bottom = ($panels | ForEach-Object { $_.gridPos.y + $_.gridPos.h } | Measure-Object -Maximum).Maximum
            $id = 1 + ($panels.id | Measure-Object -Maximum).Maximum
            $Dashboard.panels += [ordered]@{
                id = [int]$id; title = 'Field definitions'; description = ''; type = 'text'
                gridPos = [ordered]@{ x = 0; y = $bottom; w = 24; h = $height }
                options = [ordered]@{ mode = 'markdown'; content = $markdown }
            }
        }
    }
}