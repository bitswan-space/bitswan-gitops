FROM python:3.11-bookworm
WORKDIR /src

ENV VERSION=27.3.1
RUN wget -qO- https://get.docker.com/ | sh

COPY pyproject.toml .
COPY LICENSE .
COPY README.md .
COPY app/ ./app/
COPY start.sh .

RUN pip install -e .


# Create user 1000 and set up permissions
RUN useradd -u 1000 -m -s /bin/bash user1000
RUN chown -R user1000:user1000 /src

#switch to user 1000
USER user1000

RUN git config --global user.name "gitops"
RUN git config --global user.email "gitops@gitops.com"

USER root

# Install Bitswan Automation Server CLI (latest)
RUN apt-get update && apt-get install -y curl jq tar
RUN set -e; \
  LATEST_VERSION="$(curl -sL https://api.github.com/repos/bitswan-space/bitswan-automation-server/releases/latest | jq -r .tag_name)"; \
  TMP_DIR="$(mktemp -d)"; \
  curl -L "https://github.com/bitswan-space/bitswan-automation-server/releases/download/${LATEST_VERSION}/bitswan-automation-server-${LATEST_VERSION}-linux-amd64.tar.gz" | tar -xz -C "$TMP_DIR"; \
  BIN_PATH="$(find "$TMP_DIR" -type f -perm -u+x -name 'bitswan-automation-server*' | head -n1)"; \
  if [ -z "$BIN_PATH" ]; then echo "Could not locate bitswan-automation-server binary after extraction" >&2; exit 1; fi; \
  install -m 0755 "$BIN_PATH" /usr/local/bin/bitswan-automation-server; \
  rm -rf "$TMP_DIR"

ENV BITSWAN_CADDY_HOST=http://caddy:2019

# Make startup script executable and move to system location
RUN chmod +x /src/start.sh && mv /src/start.sh /usr/local/bin/start.sh

EXPOSE 8079

CMD ["/usr/local/bin/start.sh"]
