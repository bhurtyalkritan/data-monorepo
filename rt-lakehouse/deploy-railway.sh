#!/bin/bash

# RT-Lakehouse Railway Deployment Script
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Railway CLI is installed
if ! command -v railway &> /dev/null; then
    print_status "Installing Railway CLI..."
    npm install -g @railway/cli
fi

# Login to Railway (if not already logged in)
print_status "Checking Railway authentication..."
if ! railway whoami &> /dev/null; then
    print_status "Please log in to Railway..."
    railway login
fi

# Deploy to Railway
print_status "Deploying RT-Lakehouse to Railway..."
print_warning "This will deploy a simplified demo version with mock data"

# Deploy from root directory (monorepo)
railway up

print_status "🚀 Railway deployment initiated!"
print_status "📊 Check your deployment at: https://railway.app/dashboard"
print_warning "Note: This is a demo version with mock data and limited features"

echo ""
echo "📖 Available API endpoints (once deployed):"
echo "  - Health Check: /health"
echo "  - API Docs: /docs"  
echo "  - Events: /api/events"
echo "  - Metrics: /api/analytics/metrics"
echo "  - Demo Status: /api/demo/status"
