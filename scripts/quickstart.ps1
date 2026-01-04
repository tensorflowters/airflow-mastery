# Airflow Mastery - One-Command Quickstart (Windows PowerShell)
# Usage: irm https://raw.githubusercontent.com/YOUR_ORG/airflow-mastery/main/scripts/quickstart.ps1 | iex

$ErrorActionPreference = "Stop"

# ============================================
# Configuration
# ============================================

$REPO_URL = "https://github.com/YOUR_ORG/airflow-mastery.git"
$INSTALL_DIR = "$env:USERPROFILE\airflow-mastery"
$DOCS_URL = "https://YOUR_ORG.github.io/airflow-mastery"
$AIRFLOW_URL = "http://localhost:8080"

# ============================================
# Helper Functions
# ============================================

function Write-Step {
    param([string]$Message)
    Write-Host "==> " -ForegroundColor Blue -NoNewline
    Write-Host $Message
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ " -ForegroundColor Green -NoNewline
    Write-Host $Message
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ " -ForegroundColor Yellow -NoNewline
    Write-Host $Message
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ " -ForegroundColor Red -NoNewline
    Write-Host $Message
}

function Test-Command {
    param([string]$Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Wait-ForHealthy {
    param([string]$Url, [int]$MaxAttempts = 60)

    Write-Host "Waiting for Airflow to be healthy " -NoNewline

    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            $response = Invoke-RestMethod -Uri "$Url/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($response.metadatabase.status -eq "healthy") {
                Write-Host ""
                return $true
            }
        } catch {}

        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
    }

    Write-Host ""
    return $false
}

# ============================================
# Banner
# ============================================

Write-Host ""
Write-Host "╔═══════════════════════════════════════════╗" -ForegroundColor Blue
Write-Host "║     ✈️  Airflow Mastery Quickstart         ║" -ForegroundColor Blue
Write-Host "║     Complete Apache Airflow 3.x Course     ║" -ForegroundColor Blue
Write-Host "╚═══════════════════════════════════════════╝" -ForegroundColor Blue
Write-Host ""

# ============================================
# Step 1: Check Prerequisites
# ============================================

Write-Step "Checking prerequisites..."

# Check Git
if (Test-Command "git") {
    Write-Success "Git installed"
} else {
    Write-Error "Git not found. Please install Git first."
    Write-Host "    Visit: https://git-scm.com/download/win"
    exit 1
}

# Check Docker
if (Test-Command "docker") {
    try {
        docker info 2>&1 | Out-Null
        Write-Success "Docker installed and running"
    } catch {
        Write-Error "Docker is installed but not running. Please start Docker Desktop."
        exit 1
    }
} else {
    Write-Error "Docker not found. Please install Docker Desktop first."
    Write-Host "    Visit: https://www.docker.com/products/docker-desktop/"
    exit 1
}

# Check Docker Compose
try {
    docker compose version 2>&1 | Out-Null
    Write-Success "Docker Compose available"
} catch {
    Write-Error "Docker Compose not found."
    exit 1
}

Write-Host ""

# ============================================
# Step 2: Clone Repository
# ============================================

Write-Step "Setting up project..."

if (Test-Path $INSTALL_DIR) {
    Write-Warning "Directory already exists: $INSTALL_DIR"
    $response = Read-Host "    Remove and reinstall? [y/N]"
    if ($response -eq "y" -or $response -eq "Y") {
        Remove-Item -Recurse -Force $INSTALL_DIR
    } else {
        Write-Step "Using existing installation"
        Set-Location $INSTALL_DIR
    }
}

if (-not (Test-Path $INSTALL_DIR)) {
    git clone --depth 1 $REPO_URL $INSTALL_DIR
    Write-Success "Repository cloned"
}

Set-Location $INSTALL_DIR

# ============================================
# Step 3: Install uv
# ============================================

Write-Step "Setting up Python environment..."

if (Test-Command "uv") {
    Write-Success "uv already installed"
} else {
    Write-Step "Installing uv..."
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

    # Refresh PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")

    if (Test-Command "uv") {
        Write-Success "uv installed"
    } else {
        Write-Error "Failed to install uv. Please restart PowerShell and try again."
        exit 1
    }
}

# ============================================
# Step 4: Install Dependencies
# ============================================

Write-Step "Installing dependencies..."
uv sync --quiet
Write-Success "Dependencies installed"

Write-Host ""

# ============================================
# Step 5: Start Airflow
# ============================================

Write-Step "Starting Airflow..."

# Check if already running
try {
    $health = Invoke-RestMethod -Uri "$AIRFLOW_URL/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
    Write-Success "Airflow already running"
} catch {
    docker compose -f infrastructure/docker-compose/docker-compose.yml up -d

    Write-Step "Waiting for Airflow to start (this may take 1-2 minutes on first run)..."

    if (Wait-ForHealthy -Url $AIRFLOW_URL) {
        Write-Success "Airflow is healthy"
    } else {
        Write-Error "Airflow failed to start. Check logs with:"
        Write-Host "    docker compose -f infrastructure/docker-compose/docker-compose.yml logs"
        exit 1
    }
}

Write-Host ""

# ============================================
# Step 6: Open Browser
# ============================================

Write-Step "Opening browser..."

Start-Sleep -Seconds 1

# Open Airflow UI
Start-Process $AIRFLOW_URL
Write-Success "Airflow UI: $AIRFLOW_URL"

# Open docs
Start-Process $DOCS_URL
Write-Success "Documentation: $DOCS_URL"

Write-Host ""

# ============================================
# Success!
# ============================================

Write-Host "╔═══════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║     🎉 Setup Complete!                     ║" -ForegroundColor Green
Write-Host "╚═══════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "  📍 Project location: $INSTALL_DIR"
Write-Host "  🌐 Airflow UI:       $AIRFLOW_URL"
Write-Host "     Username:         admin"
Write-Host "     Password:         admin"
Write-Host "  📚 Documentation:    $DOCS_URL"
Write-Host ""
Write-Host "  Next steps:"
Write-Host "  1. cd $INSTALL_DIR"
Write-Host "  2. Start learning with Module 00!"
Write-Host ""
Write-Host "  Useful commands:"
Write-Host "    just up       - Start Airflow"
Write-Host "    just down     - Stop Airflow"
Write-Host "    just logs     - View logs"
Write-Host "    just docs     - Serve docs locally"
Write-Host ""
