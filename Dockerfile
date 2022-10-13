FROM python:3.10-slim
MAINTAINER Paul Gessinger <hello@paulgessinger.com>

RUN pip install --no-cache-dir poetry gunicorn

ENV APP_PATH /app
WORKDIR $APP_PATH

COPY pyproject.toml /app/pyproject.toml
COPY poetry.lock /app/poetry.lock

RUN poetry export -o requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app


