<#
    .SCRIPT SYNOPSIS 
        Analyzes the azcopy log, and collects transfer metrics to generate a repot
	  
	.Description
	  	This script will collect transfer metrics from azcopy log and generate a csv

	.Parameter AzCopy log
		Mandatory: This item is the Resource Group Name that will be used
		Alias: F

	.Example
		.\AzcopyThroughput.ps1 -F "C:\Users\tml\.azcopy\f2435d64-57ea-6949-7e62-006aa4ecf930.log"
                                          
    .Author  
        Tiago Limpo
		Created By Tiago Limpo
		Email tilimpo@microsoft.com		

    .Credits

    .Notes / Versions / Output
    	* Version: 1.0
		  Date: March 24th 2023
		  Purpose/Change:	analyze the Azcopy log and collect information about the transfer
          # Constrains / Pre-requisites:
            > have azcopy log

          # Output
            > Creates a csv file
            > Creates a html report file
#>

Param(
    [Parameter(Mandatory=$true, HelpMessage="Enter the full path to azcopy log file to be used on report creation and Press <Enter>")]
    [Alias('F')]
    [String]$path
    )
 
function SetCache ()
{
    param
      (
        [Parameter(Mandatory=$True)]
        [Alias('Cache')]
        [Int]$IntCache
      )
    
    Switch ($IntCache)
    {
        0 {"None"}
        1 {"ReadOnly"}
        2 {"ReadWrite"}
        default {"None"}
    }
 }
# Validate if folder
$folder = "C:\temp"
if (Test-Path -Path $folder) {}else{
    New-item -Path "C:\temp" -ItemType Directory
}
# Import the azcopy log
Write-Host "`n Loading Azcopy log ($path)......" -ForegroundColor Yellow

$timer=[system.diagnostics.stopwatch]::StartNew()
$logname = (Get-ChildItem $path).BaseName

# Read log, and save into auxiliar file, with all the information about azcoy metrics
$reader = [IO.File]::OpenText($path)
while ($reader.Peek() -ge 0) {
    $line = $reader.ReadLine()
    if ($line -match 'Mb/s'){
        $line | Out-File -FilePath C:\temp\auxiliarfile_$logname.log -Append
    }elseif($line -match 'Total Number of Transfers'){
        $totalfiles = $line
        break
    }
}
$reader.Dispose()
# Get the total files to transfer
$tf=$totalfiles -split ':', 2

# Convert the auxiliar file in a CSV to be imported in HTML report
$azcopyvalues = @()
$azvalues = Get-Content -Path "C:\temp\auxiliarfile_$logname.log"
foreach ($azvalue in $azvalues){
    $item = New-Object -TypeName PSObject
    $params=$azvalue -split ',', 7
    foreach ($param in $params[0]){
        $value=$param -split ' ', 3
        $item | Add-Member -MemberType NoteProperty -Name "timestamp" -Value $value[1]
    }
    # calculate the Percentage base on done files
    foreach ($param in $params[1]){
        $value = $param -split ' ', 3
        $value = [math]::Round(([int]$value[1]*100)/[int]$tf[1], 2)
        $item | Add-Member -MemberType NoteProperty -Name "Percentage" -Value $value
    }
    foreach ($param in $params[6]){
        $value = $param -split ':', 2
        $item | Add-Member -MemberType NoteProperty -Name "Throughput (Mb/s)" -Value $value[1]
    }
    $azcopyvalues += $item
}

$azcopyvalues | Export-Csv -NoClobber -NoTypeInformation -Encoding Unicode -Path C:\temp\ReporAzCopy_$logname.csv -Delimiter ";"
Remove-Item C:\temp\auxiliarfile_$logname.log

$timer.Stop()
Write-Host 'Duration:' $timer.Elapsed -ForegroundColor Green
