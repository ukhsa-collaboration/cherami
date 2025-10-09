FROM python:3.13.8-slim-trixie
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ADD . /cherami

WORKDIR /cherami
RUN uv sync --locked

CMD ["uv", "run", "cherami"]