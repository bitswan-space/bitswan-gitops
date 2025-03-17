# Bitswan GitOps

Bitswan gitops is a service that manages the deployment, management and monitoring of bitswan automations. With it, you can deploy a bitswan Jupyter notebook into production with the click of a button.

## Installation

For installation use the [`bitswan-gitops-cli`](https://github.com/bitswan-space/bitswan-gitops-cli)

## Development
1. Be sure external docker network `bitswan_network` is created, otherwise create it
```bash
docker network create bitswan_network  
```
2. Copy `.env.example` env file and fill slugs
```bash
cp .env.example .env
```
### Mac
```bash
docker compose --env-file .env -f docker-compose.mac.yaml up  -d  
```
### Linux
```bash
docker compose --env-file .env -f docker-compose.linux.yaml up -d
```
