#!/usr/bin/env bash
# Airflow Mastery - One-Command Quickstart
# Usage: curl -fsSL https://raw.githubusercontent.com/YOUR_ORG/airflow-mastery/main/scripts/quickstart.sh | bash

set -euo pipefail

# ============================================
# Configuration
# ============================================

REPO_URL="https://github.com/YOUR_ORG/airflow-mastery.git"
INSTALL_DIR="${HOME}/airflow-mastery"
DOCS_URL="https://YOUR_ORG.github.io/airflow-mastery"
AIRFLOW_URL="http://localhost:8080"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================
# Helper Functions
# ============================================

print_step() {
    echo -e "${BLUE}==>${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

check_command() {
    if command -v "$1" &> /dev/null; then
        return 0
    else
        return 1
    fi
}

open_url() {
    local url="$1"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$url" 2>/dev/null || true
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        xdg-open "$url" 2>/dev/null || true
    fi
}

wait_for_healthy() {
    local url="$1"
    local max_attempts=60
    local attempt=1

    echo -n "Waiting for Airflow to be healthy "
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url/health" | grep -q "healthy"; then
            echo ""
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    echo ""
    return 1
}

# ============================================
# Banner
# ============================================

echo ""
echo -e "${BLUE}╔═══════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║${NC}     ✈️  Airflow Mastery Quickstart         ${BLUE}║${NC}"
echo -e "${BLUE}║${NC}     Complete Apache Airflow 3.x Course     ${BLUE}║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════╝${NC}"
echo ""

# ============================================
# Step 1: Check Prerequisites
# ============================================

print_step "Checking prerequisites..."

# Check Git
if check_command git; then
    print_success "Git installed"
else
    print_error "Git not found. Please install Git first."
    exit 1
fi

# Check Docker
if check_command docker; then
    if docker info &> /dev/null; then
        print_success "Docker installed and running"
    else
        print_error "Docker is installed but not running. Please start Docker."
        exit 1
    fi
else
    print_error "Docker not found. Please install Docker first."
    echo "    Visit: https://www.docker.com/products/docker-desktop/"
    exit 1
fi

# Check Docker Compose
if docker compose version &> /dev/null; then
    print_success "Docker Compose available"
else
    print_error "Docker Compose not found. Please install Docker Compose."
    exit 1
fi

# Check disk space (need at least 5GB)
if [[ "$OSTYPE" == "darwin"* ]]; then
    available_gb=$(df -g "$HOME" | awk 'NR==2 {print $4}')
else
    available_gb=$(df -BG "$HOME" | awk 'NR==2 {print $4}' | tr -d 'G')
fi

if [ "$available_gb" -lt 5 ]; then
    print_warning "Low disk space: ${available_gb}GB available (5GB recommended)"
fi

echo ""

# ============================================
# Step 2: Clone Repository
# ============================================

print_step "Setting up project..."

if [ -d "$INSTALL_DIR" ]; then
    print_warning "Directory already exists: $INSTALL_DIR"
    read -p "    Remove and reinstall? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "$INSTALL_DIR"
    else
        print_step "Using existing installation"
        cd "$INSTALL_DIR"
    fi
fi

if [ ! -d "$INSTALL_DIR" ]; then
    git clone --depth 1 "$REPO_URL" "$INSTALL_DIR"
    print_success "Repository cloned"
fi

cd "$INSTALL_DIR"

# ============================================
# Step 3: Install uv
# ============================================

print_step "Setting up Python environment..."

if check_command uv; then
    print_success "uv already installed"
else
    print_step "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Add to PATH for this session
    export PATH="$HOME/.local/bin:$PATH"

    if check_command uv; then
        print_success "uv installed"
    else
        print_error "Failed to install uv"
        exit 1
    fi
fi

# ============================================
# Step 4: Install Dependencies
# ============================================

print_step "Installing dependencies..."
uv sync --quiet
print_success "Dependencies installed"

echo ""

# ============================================
# Step 5: Start Airflow
# ============================================

print_step "Starting Airflow..."

# Check if already running
if curl -s "$AIRFLOW_URL/health" &> /dev/null; then
    print_success "Airflow already running"
else
    docker compose -f infrastructure/docker-compose/docker-compose.yml up -d

    print_step "Waiting for Airflow to start (this may take 1-2 minutes on first run)..."

    if wait_for_healthy "$AIRFLOW_URL"; then
        print_success "Airflow is healthy"
    else
        print_error "Airflow failed to start. Check logs with: docker compose -f infrastructure/docker-compose/docker-compose.yml logs"
        exit 1
    fi
fi

echo ""

# ============================================
# Step 6: Open Browser
# ============================================

print_step "Opening browser..."

sleep 1

# Open Airflow UI
open_url "$AIRFLOW_URL"
print_success "Airflow UI: $AIRFLOW_URL"

# Open docs
open_url "$DOCS_URL"
print_success "Documentation: $DOCS_URL"

echo ""

# ============================================
# Success!
# ============================================

echo -e "${GREEN}╔═══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║${NC}     🎉 Setup Complete!                     ${GREEN}║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════╝${NC}"
echo ""
echo "  📍 Project location: $INSTALL_DIR"
echo "  🌐 Airflow UI:       $AIRFLOW_URL"
echo "     Username:         admin"
echo "     Password:         admin"
echo "  📚 Documentation:    $DOCS_URL"
echo ""
echo "  Next steps:"
echo "  1. cd $INSTALL_DIR"
echo "  2. Start learning with Module 00!"
echo ""
echo "  Useful commands:"
echo "    just up       - Start Airflow"
echo "    just down     - Stop Airflow"
echo "    just logs     - View logs"
echo "    just docs     - Serve docs locally"
echo ""
