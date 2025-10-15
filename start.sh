#!/bin/bash

# Function to get the group ID of the docker socket
get_docker_gid() {
    if [ -S /var/run/docker.sock ]; then
        # Get the group ID of the docker socket
        stat -c %g /var/run/docker.sock
    else
        # Fallback to docker group if socket doesn't exist
        getent group docker | cut -d: -f3
    fi
}

# Function to ensure user1000 is in the docker group
setup_docker_group() {
    local docker_gid=$(get_docker_gid)
    
    if [ -n "$docker_gid" ]; then
        echo "Docker socket group ID: $docker_gid"
        
        # Check if docker group exists with this GID
        if ! getent group $docker_gid >/dev/null 2>&1; then
            echo "Creating docker group with GID $docker_gid"
            groupadd -g $docker_gid docker
        fi
        
        # Add user1000 to the docker group
        echo "Adding user1000 to docker group (GID: $docker_gid)"
        usermod -aG docker user1000
        
        # Update the docker group GID to match the socket
        groupmod -g $docker_gid docker 2>/dev/null || true
    else
        echo "Warning: Could not determine docker group ID"
    fi
}

chown -R 1000 /gitops/gitops/

# Main execution
if [ -z "$HOST_PATH" ] && [ -z "$HOST_HOME" ] && [ -z "$HOST_USER" ]; then
    setup_docker_group
    mkdir -p /var/log/internal-image-build
    chown -R user1000:user1000 /var/log/internal-image-build
    chown -R user1000:user1000 /home/user1000

    # Setup SSH known_hosts for GitHub to avoid manual verification
    echo "Setting up SSH known_hosts for GitHub"
    mkdir -p /home/user1000/.ssh
    if [ ! -f /home/user1000/.ssh/known_hosts ] || ! grep -q "github.com" /home/user1000/.ssh/known_hosts; then
        ssh-keyscan -H github.com >> /home/user1000/.ssh/known_hosts 2>/dev/null
    fi
    chmod 700 /home/user1000/.ssh
    chmod 600 /home/user1000/.ssh/known_hosts 2>/dev/null || true
    chown -R user1000:user1000 /home/user1000/.ssh

    echo "Running as user1000"
    exec su -s /bin/bash user1000 -c "bitswan-gitops-server"
else
    echo "Environment variables set, running as root"
    exec bitswan-gitops-server
fi
