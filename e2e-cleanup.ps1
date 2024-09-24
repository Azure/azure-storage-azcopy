Set-PSDebug -Trace 1

$rgs = Get-AzResourceGroup -Name azcopy-newe2e-*

$rmTarget = @()

foreach($rg in $rgs) {
    if ($rg.Tags)
    {
        $unixTime = $rg.Tags["creation"]

        if ($unixTime) {
            $date = (Get-Date 01.01.1970)+[System.TimeSpan]::FromSeconds($unixTime)
            if ($date.AddDays(1).CompareTo((Get-Date)) -eq -1) {
                $rmTarget = $rmTarget + @($rg)
            }
        } else {
            # If it isn't present, it's considered too old and needs to be deleted.
            $rmTarget = $rmTarget + @($rg)
        }
    } else {
        $rmTarget = $rmTarget + @($rg)
    }
}

foreach ($rg in $rmTarget) {
    $rgn = $rg.ResourceGroupName
    Write-Output "Removing $rgn"
    Remove-AzResourceGroup -Name $rgn -Force
}