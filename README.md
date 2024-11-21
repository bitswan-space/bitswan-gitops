# Bitswan GitOps
## Development
## Deployment
### Windows/MacOS
```yaml
  gitops:
    image: bitswan/gitops:2024-11951928728-git-c8442e6
    restart: always
    ports:
      - "8079:8079"
    volumes:
      - /path/to/repo:/repo # Change this to the path of your repository
      - /var/run/docker.sock:/var/run/docker.sock
      - /etc/bitswan-secrets/:/etc/bitswan-secrets/
      - ${HOME}/.gitconfig:/root/.gitconfig
    environment:
      - BS_BITSWAN_DIR=/repo
      - BS_HOST_DIR=/path/to/repo # Change this to the path of your repository
      - MQTT_BROKER=emqx
      - MQTT_TOPIC=/c/local/running-pipelines/topology
```
### Linux
```yaml
  gitops:
    image: bitswan/gitops:2024-11951928728-git-c8442e6
    restart: always
    privileged: true
    pid: host
    ports:
      - "8079:8079"
    volumes:
      - /path/to/repo:/repo # Change this to the path of your repository
      - /var/run/docker.sock:/var/run/docker.sock
      - /etc/bitswan-secrets/:/etc/bitswan-secrets/
    environment:
      - BS_BITSWAN_DIR=/repo
      - BS_HOST_DIR=/path/to/repo # Change this to the path of your repository
      - MQTT_BROKER=mosquitto
      - MQTT_TOPIC=/c/local/running-pipelines/topology
      - HOST_PATH: $PATH
      - HOST_HOME: $HOME
      - HOST_USER: $USER
```