FROM python:3.11-bookworm
WORKDIR /src

ENV VERSION=27.3.1
RUN wget -qO- https://get.docker.com/ | sh

COPY pyproject.toml .
COPY LICENSE .
COPY README.md .
COPY app/ ./app/

RUN pip install -e .

# Create user 1000 and set up permissions
RUN useradd -u 1000 -m -s /bin/bash user1000 && usermod -aG docker user1000
RUN chown -R user1000:user1000 /src

# Create startup script
RUN echo '#!/bin/bash\n\
if [ -z "$HOST_PATH" ] && [ -z "$HOST_HOME" ] && [ -z "$HOST_USER" ]; then\n\
    echo "Environment variables not set, switching to user 1000"\n\
    exec su user1000 -c "bitswan-gitops-server"\n\
else\n\
    echo "Environment variables set, running as root"\n\
    exec bitswan-gitops-server\n\
fi' > /usr/local/bin/start.sh && chmod +x /usr/local/bin/start.sh

EXPOSE 8079

CMD ["/usr/local/bin/start.sh"]
