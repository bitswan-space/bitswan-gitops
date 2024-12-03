FROM python:3.11-bookworm
WORKDIR /src

ENV VERSION=27.3.1
RUN wget -qO- https://get.docker.com/ | sh

COPY pyproject.toml .
COPY LICENSE .
COPY README.md .
COPY app/ ./app/

RUN pip install -e .
EXPOSE 8079

CMD ["bitswan-gitops-server"]
