#!/usr/bin/env pwsh
# .agent/hooks/pre-commit.ps1
# Analyzes the repository for language/stack signatures and dynamically synchronizes agent skills.

$repoRoot = git rev-parse --show-toplevel
if ($LASTEXITCODE -ne 0) { exit 0 }
Set-Location -Path $repoRoot

# Pre-commit safety guard: Auto-destage any accidental .sst or .wal files from Git index (disk preserved)
$stagedDBFiles = git diff --cached --name-only -- "*.sst" "*.wal" 2>$null
if ($stagedDBFiles) {
    $fileList = @($stagedDBFiles -split "`r?`n" | Where-Object { $_.Trim() -ne "" })
    if ($fileList.Count -gt 0) {
        Write-Host "Pre-Commit Guard: Detected $($fileList.Count) database file(s) staged in Git index." -ForegroundColor Yellow
        foreach ($f in $fileList) {
            git reset HEAD -- $f 2>$null
            Write-Host "  Auto-destaged from commit: $f (local disk file preserved)" -ForegroundColor Yellow
        }
        Write-Host "Pre-Commit Guard: Database files safely unstaged. (Use 'make clean-sst' to physically delete them)." -ForegroundColor Cyan
    }
}

$registryMapPath = ".agent\.registry\skill-map.json"
$activeSkillsPath = ".agent\.active-skills.json"
$skillsDir = ".agent\skills"

# Exit silently if the vault isn't vendored yet
if (-not (Test-Path $registryMapPath)) { exit 0 }

$map = Get-Content $registryMapPath -Raw | ConvertFrom-Json

# Initialize or load state matching the workspace-initializer metadata schema
$state = [ordered]@{
    version        = "1.0.0"
    initialized_at = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
    stack          = "auto"
    active_skills  = @()
}

if (Test-Path $activeSkillsPath) {
    try {
        $loadedState = Get-Content $activeSkillsPath -Raw | ConvertFrom-Json
        if ($loadedState.active_skills) {
            $state.active_skills = @($loadedState.active_skills)
            if ($loadedState.stack) { $state.stack = $loadedState.stack }
            if ($loadedState.initialized_at) { $state.initialized_at = $loadedState.initialized_at }
            if ($loadedState.version) { $state.version = $loadedState.version }
        }
    }
    catch {}
}

$requiredSkills = @()

# Scan repository signatures directly from map properties (root and subdirectories)
foreach ($prop in $map.psobject.properties) {
    if (Test-Path $prop.Name) {
        $requiredSkills += $prop.Value
    }
    elseif (-not $prop.Name.EndsWith("/") -and (Get-ChildItem -Path "." -Filter $prop.Name -Recurse -File -Depth 4 -ErrorAction SilentlyContinue | Select-Object -First 1)) {
        $requiredSkills += $prop.Value
    }
}
$requiredSkills = $requiredSkills | Select-Object -Unique

$driftDetected = $false

if (-not (Test-Path $skillsDir)) {
    New-Item -ItemType Directory -Path $skillsDir -Force | Out-Null
}

foreach ($req in $requiredSkills) {
    if ($req -notin $state.active_skills) {
        $folderName = Split-Path $req -Leaf
        $globalSkills = @('code-review', 'doc-sync', 'error-discipline', 'git-release', 'security-auditor', 'test-discipline', 'workspace-initializer')
        if ($folderName -in $globalSkills) { continue }

        $linkPath = Join-Path $skillsDir $folderName
        $targetPath = Join-Path $repoRoot ".agent\.registry\$req"

        if (-not (Test-Path $targetPath)) { continue }

        Write-Host "Antigravity Sync: Linking skill -> $folderName ($req)" -ForegroundColor Cyan
        
        if (-not (Test-Path $linkPath)) {
            try {
                New-Item -ItemType SymbolicLink -Path $linkPath -Target $targetPath -ErrorAction Stop | Out-Null
            }
            catch {
                # Fallback to NTFS junction on Windows if Developer Mode / SeCreateSymbolicLinkPrivilege is absent
                cmd.exe /c mklink /J "$linkPath" "$targetPath" | Out-Null
            }
        }

        $state.active_skills += $req
        $driftDetected = $true
    }
}

if ($driftDetected) {
    $state | ConvertTo-Json -Depth 4 | Set-Content -Path $activeSkillsPath -Encoding utf8
    Write-Host "Antigravity Sync: Active skills updated. Staging tracker..." -ForegroundColor Green
    git add $activeSkillsPath
}

exit 0