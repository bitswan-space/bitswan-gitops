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

# Make startup script executable and move to system location
RUN chmod +x /src/start.sh && mv /src/start.sh /usr/local/bin/start.sh

EXPOSE 8079

CMD ["/usr/local/bin/start.sh"]
