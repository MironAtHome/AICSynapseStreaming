Param(
    [Parameter(Mandatory = $true)]
    [String] $JdkVersion,
    [Parameter(Mandatory = $true)]
    [String] $JavaRoot
)

$url = "https://cdn.azul.com/zulu/bin/$JdkVersion.zip"
$fileNameWithPath = ".\OpenJDK.zip"

$ErrorActionPreference = "Stop"

Write-Host "Downloading JAVA 8 from $url ..."
(New-Object System.Net.WebClient).DownloadFile($url, $fileNameWithPath)
Write-Host "Download complete to $fileNameWithPath."

Expand-Archive -Path $fileNameWithPath -DestinationPath "$JavaRoot\" -Force -ErrorAction Stop