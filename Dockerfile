FROM python:3.10-slim-bullseye
MAINTAINER Paul Gessinger <hello@paulgessinger.com>

RUN apt-get update && apt-get install -y poppler-utils && apt-get clean

RUN pip install --no-cache-dir poetry hypercorn

ENV APP_PATH /app
WORKDIR $APP_PATH

COPY pyproject.toml /app/pyproject.toml
COPY poetry.lock /app/poetry.lock

RUN poetry export -o requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN pip install .
