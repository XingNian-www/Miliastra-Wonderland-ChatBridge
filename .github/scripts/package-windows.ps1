param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9]+[.][0-9]+[.][0-9]+(?:-[0-9A-Za-z.-]+)?(?:[+][0-9A-Za-z.-]+)?$')]
    [string] $Version
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Copy only versioned release inputs. Runtime metadata is reused from the existing distribution.
$files = [ordered] @{
    'miliastra-wonderland-chatbridge.exe' = 'target/x86_64-pc-windows-msvc/release/miliastra-wonderland-chatbridge.exe'
    'MiliastraWonderlandChatBridge.Native.dll' = 'build/native/MiliastraWonderlandChatBridge.Native.dll'
    'metadata.bin' = 'resources/metadata.bin'
    'LICENSE' = 'LICENSE'
    'THIRD_PARTY_LICENSES' = 'THIRD_PARTY_LICENSES'
    'README.md' = 'README.md'
    'docs/API.md' = 'docs/API.md'
}
$stage = Join-Path 'build' ('package-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path (Join-Path $stage 'docs') -Force | Out-Null
foreach ($file in $files.GetEnumerator()) {
    if (-not (Test-Path -LiteralPath $file.Value -PathType Leaf)) {
        throw "Required distribution file missing: $($file.Value)"
    }
    Copy-Item -LiteralPath $file.Value -Destination (Join-Path $stage $file.Key)
}

$release = New-Item -ItemType Directory -Path 'build/release' -Force
$zipName = "miliastra-wonderland-chatbridge-v$Version-windows-x64.zip"
$zipPath = Join-Path $release.FullName $zipName
Compress-Archive -Path (Join-Path $stage '*') -DestinationPath $zipPath -Force

# Verify the archive itself, including nested paths, before publishing it.
$archive = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
try {
    $actual = @($archive.Entries | Where-Object { $_.Name -ne '' } |
        ForEach-Object { $_.FullName.Replace('\', '/') } | Sort-Object)
    $expected = @($files.Keys | Sort-Object)
    if ($actual.Count -ne $expected.Count -or
        @(Compare-Object -ReferenceObject $expected -DifferenceObject $actual -CaseSensitive).Count -ne 0) {
        throw "Unexpected ZIP contents: $($actual -join ', ')"
    }
} finally {
    $archive.Dispose()
}
$hash = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
"$hash  $zipName" | Set-Content -LiteralPath (Join-Path $release.FullName 'SHA256SUMS') -Encoding ascii
Write-Host "Packaged $zipName (includes versioned metadata.bin). SHA256: $hash"
