# setup.ps1 - Antigravity Agent & Git Hook Setup
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Write-Host "Configuring Antigravity Workspace..." -ForegroundColor Cyan

# 1. Verify Git availability and repository state
if (-not (Test-Path ".git")) {
    Write-Error "Not a git repository. Run 'git init' before setup."
    exit 1
}

# 2. Initialize or vendor .agent/.registry submodule
if (-not (Test-Path ".agent/.registry/skill-map.json")) {
    Write-Host "Initializing .agent/.registry submodule from vault..." -ForegroundColor Yellow
    git submodule update --init --recursive
    if (-not (Test-Path ".agent/.registry/skill-map.json")) {
        try {
            git submodule add --force https://github.com/Awesome06/agent-skills-vault.git .agent/.registry
        } catch {
            Write-Warning "Could not automatically add submodule. If offline or uncommitted, run 'git submodule update --init' later."
        }
    }
}

# 3. Configure Git pre-commit hook
$hooksDir = ".git/hooks"
$gitHookPath = "$hooksDir/pre-commit"

if (-not (Test-Path $hooksDir)) {
    New-Item -ItemType Directory -Path $hooksDir -Force | Out-Null
}

$agentTrigger = @"
# --- Antigravity Agent Hook Start ---
if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File .agent/hooks/pre-commit.ps1 || exit 1
elif command -v pwsh >/dev/null 2>&1; then
    pwsh -NoProfile -NonInteractive -File .agent/hooks/pre-commit.ps1 || exit 1
elif [ -f .agent/hooks/pre-commit.sh ]; then
    sh .agent/hooks/pre-commit.sh || exit 1
fi
# --- Antigravity Agent Hook End ---
"@ -replace "`r`n", "`n"

if (Test-Path $gitHookPath) {
    $existing = (Get-Content $gitHookPath -Raw) -replace "`r`n", "`n"
    if ($existing -notmatch "Antigravity Agent Hook") {
        $updated = "$existing`n$agentTrigger`n"
        [System.IO.File]::WriteAllText((Convert-Path $gitHookPath), $updated, (New-Object System.Text.UTF8Encoding($false)))
        Write-Host "Appended Antigravity sync to existing pre-commit hook." -ForegroundColor Green
    } else {
        Write-Host "Antigravity sync is already configured in the pre-commit hook." -ForegroundColor Yellow
    }
} else {
    $newHook = "#!/bin/sh`n$agentTrigger`n"
    [System.IO.File]::WriteAllText((Join-Path (Get-Location) $gitHookPath), $newHook, (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "Created pre-commit hook with Antigravity sync (LF encoded)." -ForegroundColor Green
}

# 4. Run initial skill discovery immediately
if (Test-Path ".agent/hooks/pre-commit.ps1") {
    Write-Host "Running initial skill synchronization..." -ForegroundColor Cyan
    & ".agent/hooks/pre-commit.ps1"
}

Write-Host "Workspace ready. Agent skills will validate and sync on every commit." -ForegroundColor Cyan
