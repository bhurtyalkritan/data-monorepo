#!/bin/bash
# RT-Lakehouse deployment script for production environments

set -e

# Configuration
PROJECT_NAME="rt-lakehouse"
DOCKER_REGISTRY="ghcr.io"
NAMESPACE="${NAMESPACE:-rt-lakehouse}"
ENVIRONMENT="${ENVIRONMENT:-production}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
    exit 1
}

# Validate required environment variables
validate_environment() {
    log "Validating environment variables..."
    
    required_vars=(
        "GITHUB_SHA"
        "ENVIRONMENT"
    )
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            error "Required environment variable $var is not set"
        fi
    done
    
    log "Environment validation passed"
}

# Pre-deployment health checks
pre_deployment_checks() {
    log "Running pre-deployment checks..."
    
    # Check if Docker is running
    if ! docker info > /dev/null 2>&1; then
        error "Docker is not running or not accessible"
    fi
    
    # Check if required images exist
    services=("assistant" "frontend" "monitoring" "producer" "spark")
    for service in "${services[@]}"; do
        image="${DOCKER_REGISTRY}/${GITHUB_REPOSITORY}/${PROJECT_NAME}-${service}:${GITHUB_SHA}"
        log "Checking image: $image"
        
        if ! docker manifest inspect "$image" > /dev/null 2>&1; then
            error "Image $image not found in registry"
        fi
    done
    
    log "Pre-deployment checks passed"
}

# Deploy using Docker Compose
deploy_docker_compose() {
    log "Deploying RT-Lakehouse with Docker Compose..."
    
    # Create environment-specific compose file
    cat > docker-compose.${ENVIRONMENT}.yml << EOF
version: '3.8'

services:
  assistant-api:
    image: ${DOCKER_REGISTRY}/${GITHUB_REPOSITORY}/${PROJECT_NAME}-assistant:${GITHUB_SHA}
    restart: unless-stopped
    
  frontend:
    image: ${DOCKER_REGISTRY}/${GITHUB_REPOSITORY}/${PROJECT_NAME}-frontend:${GITHUB_SHA}
    restart: unless-stopped
    
  monitoring:
    image: ${DOCKER_REGISTRY}/${GITHUB_REPOSITORY}/${PROJECT_NAME}-monitoring:${GITHUB_SHA}
    restart: unless-stopped
    
  producer:
    image: ${DOCKER_REGISTRY}/${GITHUB_REPOSITORY}/${PROJECT_NAME}-producer:${GITHUB_SHA}
    restart: unless-stopped
    
  spark-streaming:
    image: ${DOCKER_REGISTRY}/${GITHUB_REPOSITORY}/${PROJECT_NAME}-spark:${GITHUB_SHA}
    restart: unless-stopped
EOF
    
    # Deploy with compose
    docker-compose -f docker-compose.yml -f docker-compose.${ENVIRONMENT}.yml up -d
    
    log "Docker Compose deployment completed"
}

# Deploy to Kubernetes
deploy_kubernetes() {
    log "Deploying RT-Lakehouse to Kubernetes..."
    
    # Create namespace if it doesn't exist
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    
    # Apply Kubernetes manifests
    envsubst < k8s/deployment.yaml | kubectl apply -n "$NAMESPACE" -f -
    envsubst < k8s/service.yaml | kubectl apply -n "$NAMESPACE" -f -
    
    # Wait for rollout to complete
    kubectl rollout status deployment/rt-lakehouse-assistant -n "$NAMESPACE" --timeout=300s
    kubectl rollout status deployment/rt-lakehouse-frontend -n "$NAMESPACE" --timeout=300s
    
    log "Kubernetes deployment completed"
}

# Post-deployment verification
post_deployment_checks() {
    log "Running post-deployment checks..."
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 30
    
    # Health check endpoints
    if [[ "$DEPLOYMENT_TARGET" == "kubernetes" ]]; then
        # Port forward for testing
        kubectl port-forward service/rt-lakehouse-assistant 8000:8000 -n "$NAMESPACE" &
        PORT_FORWARD_PID=$!
        sleep 5
    fi
    
    # Test API health
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        log "Health check attempt $attempt/$max_attempts"
        
        if curl -f http://localhost:8000/health > /dev/null 2>&1; then
            log "API health check passed"
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            error "API health check failed after $max_attempts attempts"
        fi
        
        sleep 10
        ((attempt++))
    done
    
    # Test metrics endpoint
    if curl -f http://localhost:8000/metrics > /dev/null 2>&1; then
        log "Metrics endpoint check passed"
    else
        warn "Metrics endpoint check failed"
    fi
    
    # Cleanup port forward if we started one
    if [[ -n "$PORT_FORWARD_PID" ]]; then
        kill $PORT_FORWARD_PID
    fi
    
    log "Post-deployment checks completed"
}

# Rollback deployment
rollback() {
    log "Rolling back deployment..."
    
    if [[ "$DEPLOYMENT_TARGET" == "kubernetes" ]]; then
        kubectl rollout undo deployment/rt-lakehouse-assistant -n "$NAMESPACE"
        kubectl rollout undo deployment/rt-lakehouse-frontend -n "$NAMESPACE"
        kubectl rollout status deployment/rt-lakehouse-assistant -n "$NAMESPACE" --timeout=300s
    else
        # Docker Compose rollback
        docker-compose down
        docker-compose -f docker-compose.yml up -d
    fi
    
    log "Rollback completed"
}

# Cleanup old resources
cleanup() {
    log "Cleaning up old resources..."
    
    if [[ "$DEPLOYMENT_TARGET" == "kubernetes" ]]; then
        # Remove old ReplicaSets
        kubectl delete replicaset -l app=rt-lakehouse -n "$NAMESPACE" --cascade=orphan
    else
        # Remove old Docker images
        docker image prune -f
    fi
    
    log "Cleanup completed"
}

# Main deployment function
main() {
    log "Starting RT-Lakehouse deployment for environment: $ENVIRONMENT"
    
    # Set deployment target based on environment
    if [[ "$ENVIRONMENT" == "production" ]]; then
        DEPLOYMENT_TARGET="kubernetes"
    else
        DEPLOYMENT_TARGET="docker-compose"
    fi
    
    log "Deployment target: $DEPLOYMENT_TARGET"
    
    # Run deployment steps
    validate_environment
    pre_deployment_checks
    
    if [[ "$DEPLOYMENT_TARGET" == "kubernetes" ]]; then
        deploy_kubernetes
    else
        deploy_docker_compose
    fi
    
    post_deployment_checks
    cleanup
    
    log "RT-Lakehouse deployment completed successfully!"
    log "Services available at:"
    log "  - API: http://localhost:8000"
    log "  - Frontend: http://localhost:3000"
    log "  - Monitoring: http://localhost:8501"
}

# Handle script arguments
case "${1:-deploy}" in
    deploy)
        main
        ;;
    rollback)
        rollback
        ;;
    cleanup)
        cleanup
        ;;
    health-check)
        post_deployment_checks
        ;;
    *)
        echo "Usage: $0 {deploy|rollback|cleanup|health-check}"
        exit 1
        ;;
esac
