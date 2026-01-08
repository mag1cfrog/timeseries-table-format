FROM python:3.11-slim

RUN apt-get update -qq \
    && apt-get install -y -qq curl \
    && pip install -q pyarrow \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
