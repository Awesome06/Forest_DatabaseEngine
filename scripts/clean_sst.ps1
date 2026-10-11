# scripts/clean_sst.ps1
# On-demand cleanup utility: Recursively purges all transient .sst database files
# across the repository and unstages any if indexed. Invoked via `make clean-sst`.

[CmdletBinding()]
param()

$repoRoot = git rev-parse --show-toplevel 2>$null
if (-not $repoRoot -or $LASTEXITCODE -ne 0) {
    $repoRoot = Get-Location
}
Set-Location -Path $repoRoot

Write-Host "SST Clean Utility: Scanning for transient .sst database files..." -ForegroundColor Cyan

# 1. Find and delete all .sst files recursively across the workspace
$sstFiles = Get-ChildItem -Path $repoRoot -Recurse -Filter "*.sst" -File -ErrorAction SilentlyContinue

$deletedCount = 0
if ($sstFiles) {
    foreach ($file in $sstFiles) {
        try {
            Remove-Item -Path $file.FullName -Force -ErrorAction Stop
            Write-Host "  Removed: $($file.FullName)" -ForegroundColor DarkGray
            $deletedCount++
        }
        catch {
            Write-Warning "  Could not remove $($file.FullName): $_"
        }
    }
}

# 2. Check if any .sst files were accidentally staged in Git index
$stagedSST = git diff --cached --name-only -- "*.sst" 2>$null
if ($stagedSST) {
    Write-Host "  Unstaging accidentally staged .sst files from Git index..." -ForegroundColor Yellow
    foreach ($stagedFile in ($stagedSST -split "`r?`n")) {
        if ($stagedFile.Trim() -ne "") {
            git reset HEAD -- $stagedFile 2>$null
            Write-Host "  Unstaged: $stagedFile" -ForegroundColor Yellow
        }
    }
}

if ($deletedCount -gt 0) {
    Write-Host "SST Clean Utility: Successfully deleted $deletedCount .sst file(s)." -ForegroundColor Green
} else {
    Write-Host "SST Clean Utility: Working directory clean. Zero .sst files found." -ForegroundColor Green
}

exit 0
