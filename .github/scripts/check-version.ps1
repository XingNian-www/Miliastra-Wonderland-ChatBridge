$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$metadataJson = & cargo +stable metadata --no-deps --format-version 1 --locked
if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed.' }
$metadata = $metadataJson | ConvertFrom-Json
$package = @($metadata.packages | Where-Object { $_.name -eq 'miliastra-wonderland-chatbridge' })
if ($package.Count -ne 1) { throw 'Expected exactly one Rust host package.' }
$version = [string] $package[0].version
[xml] $project = Get-Content -LiteralPath 'native/MiliastraWonderlandChatBridge.Native.csproj' -Raw
$nativeVersions = @($project.SelectNodes('/Project/PropertyGroup/Version'))
if ($nativeVersions.Count -ne 1) { throw 'Expected exactly one native project Version.' }
$nativeVersion = $nativeVersions[0].InnerText.Trim()
if ($version -cne $nativeVersion) {
    throw "Version mismatch: Cargo=$version, native=$nativeVersion."
}
if ($env:GITHUB_REF -like 'refs/tags/*' -and $env:GITHUB_REF -cne "refs/tags/v$version") {
    throw "Release tag must be v$version; received $env:GITHUB_REF."
}
Write-Host "Validated Rust/native version $version; ref=$env:GITHUB_REF"
if ($env:GITHUB_OUTPUT) {
    "version=$version" | Out-File -FilePath $env:GITHUB_OUTPUT -Encoding utf8 -Append
}
