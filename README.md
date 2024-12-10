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
      - /home/root/.config/bitswan/local-gitops/gitops:/home/root/.config/bitswan/gitops
      - /home/root/.config/bitswan/local-gitops/secrets:/home/root/.config/bitswan/secrets
      - /var/run/docker.sock:/var/run/docker.sock
      - ${HOME}/.gitconfig:/root/.gitconfig
    environment:
      - BITSWAN_GITOPS_DIR=/home/root/.config/bitswan
      - BITSWAN_GITOPS_DIR_HOST=/home/root/.config/bitswan/local-gitops/
      - BITSWAN_GITOPS_ID=local-gitops
      - BITSWAN_GITOPS_SECRET=secret
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
      - /home/root/.config/bitswan/local-gitops/gitops:/home/root/.config/bitswan/gitops
      - /home/root/.config/bitswan/local-gitops/secrets:/home/root/.config/bitswan/secrets
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - BITSWAN_GITOPS_DIR=/home/root/.config/bitswan
      - BITSWAN_GITOPS_DIR_HOST=/home/root/.config/bitswan/local-gitops/
      - BITSWAN_GITOPS_ID=local-gitops
      - BITSWAN_GITOPS_SECRET=secret
      - MQTT_BROKER=mosquitto
      - MQTT_TOPIC=/c/local/running-pipelines/topology
      - HOST_PATH=$PATH
      - HOST_HOME=$HOME
      - HOST_USER=$USER
```