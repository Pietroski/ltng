#!/bin/bash

# Docker-in-Docker Validation Script for Drone Pipeline
# This script validates that Docker-in-Docker (DinD) works correctly in the drone environment

set -e

echo "=== Docker-in-Docker Validation Script ==="
echo "Starting validation at $(date)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS")
            echo -e "${GREEN}✓ $message${NC}"
            ;;
        "ERROR")
            echo -e "${RED}✗ $message${NC}"
            ;;
        "WARNING")
            echo -e "${YELLOW}⚠ $message${NC}"
            ;;
        "INFO")
            echo -e "$message"
            ;;
    esac
}

# Check if Docker is available
check_docker_availability() {
    print_status "INFO" "Checking Docker availability..."
    
    if command -v docker >/dev/null 2>&1; then
        print_status "SUCCESS" "Docker command is available"
        docker --version
    else
        print_status "ERROR" "Docker command not found"
        return 1
    fi
}

# Check Docker daemon connectivity
check_docker_daemon() {
    print_status "INFO" "Checking Docker daemon connectivity..."
    
    if docker info >/dev/null 2>&1; then
        print_status "SUCCESS" "Docker daemon is accessible"
        echo "Docker Info:"
        docker info | grep -E "Server Version|Storage Driver|Kernel Version|Operating System"
    else
        print_status "ERROR" "Cannot connect to Docker daemon"
        print_status "INFO" "Checking if Docker socket is mounted..."
        if [ -S "/var/run/docker.sock" ]; then
            print_status "SUCCESS" "Docker socket is mounted at /var/run/docker.sock"
            ls -la /var/run/docker.sock
        else
            print_status "ERROR" "Docker socket not found at /var/run/docker.sock"
        fi
        return 1
    fi
}

# Test basic Docker operations
test_basic_docker_operations() {
    print_status "INFO" "Testing basic Docker operations..."
    
    # Test docker run
    if docker run --rm hello-world >/dev/null 2>&1; then
        print_status "SUCCESS" "Docker run test passed"
    else
        print_status "ERROR" "Docker run test failed"
        return 1
    fi
    
    # Test docker build
    print_status "INFO" "Testing Docker build..."
    cat > /tmp/test-dockerfile << EOF
FROM alpine:latest
RUN echo "Docker build test"
CMD echo "Hello from test container"
EOF
    
    if docker build -t test-build -f /tmp/test-dockerfile /tmp >/dev/null 2>&1; then
        print_status "SUCCESS" "Docker build test passed"
        docker rmi test-build >/dev/null 2>&1
    else
        print_status "ERROR" "Docker build test failed"
        return 1
    fi
}

# Test Docker Compose if available
test_docker_compose() {
    print_status "INFO" "Testing Docker Compose availability..."
    
    if command -v docker-compose >/dev/null 2>&1; then
        print_status "SUCCESS" "Docker Compose is available"
        docker-compose --version
        
        # Test with a simple compose file
        cat > /tmp/test-compose.yml << EOF
version: '3.8'
services:
  test:
    image: alpine:latest
    command: echo "Docker Compose test"
EOF
        
        if docker-compose -f /tmp/test-compose.yml up --abort-on-container-exit >/dev/null 2>&1; then
            print_status "SUCCESS" "Docker Compose test passed"
            docker-compose -f /tmp/test-compose.yml down >/dev/null 2>&1
        else
            print_status "WARNING" "Docker Compose test failed"
        fi
    else
        print_status "WARNING" "Docker Compose not available"
    fi
}

# Test integration with project's Docker setup
test_project_docker_integration() {
    print_status "INFO" "Testing project Docker integration..."
    
    # Check if project Dockerfiles exist
    if [ -f "build/docker/lightning-db-node.Dockerfile" ]; then
        print_status "SUCCESS" "Project Dockerfile found"
        
        # Test building the project image
        print_status "INFO" "Testing project Docker build..."
        if docker build -t lightning-db-test -f build/docker/lightning-db-node.Dockerfile . >/dev/null 2>&1; then
            print_status "SUCCESS" "Project Docker build test passed"
            docker rmi lightning-db-test >/dev/null 2>&1 || true
        else
            print_status "ERROR" "Project Docker build test failed"
            return 1
        fi
    else
        print_status "WARNING" "Project Dockerfile not found"
    fi
    
    # Check if docker-compose files exist
    if [ -f "build/orchestrator/docker-compose-test.yml" ]; then
        print_status "SUCCESS" "Project docker-compose-test.yml found"
        
        # Validate compose file syntax
        if docker-compose -f build/orchestrator/docker-compose-test.yml config >/dev/null 2>&1; then
            print_status "SUCCESS" "Docker Compose file syntax is valid"
        else
            print_status "ERROR" "Docker Compose file syntax validation failed"
        fi
    else
        print_status "WARNING" "Project docker-compose-test.yml not found"
    fi
}

# Test privileged mode (required for some DinD scenarios)
test_privileged_mode() {
    print_status "INFO" "Testing privileged mode capabilities..."
    
    # Check if we're running in privileged mode
    if docker run --rm --privileged alpine:latest sh -c "mount | grep -q cgroup" >/dev/null 2>&1; then
        print_status "SUCCESS" "Privileged mode is working"
    else
        print_status "WARNING" "Privileged mode test inconclusive"
    fi
}

# Main validation function
main() {
    echo "Starting Docker-in-Docker validation..."
    echo "========================================"
    
    local exit_code=0
    
    check_docker_availability || exit_code=1
    check_docker_daemon || exit_code=1
    test_basic_docker_operations || exit_code=1
    test_docker_compose
    test_project_docker_integration || exit_code=1
    test_privileged_mode
    
    echo "========================================"
    if [ $exit_code -eq 0 ]; then
        print_status "SUCCESS" "All Docker-in-Docker validations passed!"
        echo "Your Drone pipeline should work correctly with Docker operations."
    else
        print_status "ERROR" "Some Docker-in-Docker validations failed!"
        echo "Please check the configuration and ensure:"
        echo "  1. Docker socket is properly mounted (/var/run/docker.sock)"
        echo "  2. The pipeline step has 'privileged: true' if needed"
        echo "  3. The Docker image includes Docker client"
    fi
    
    echo "Validation completed at $(date)"
    return $exit_code
}

# Run main function
main "$@"
