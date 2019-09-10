FROM python:3.7-alpine

ENV KAFKA_BOOTSTRAP_SERVERS "kafka:9092"
ENV LOG_LEVEL "INFO"
ENV SENTRY_DSN ""
ENV POETRY_VERSION "0.12.17"

RUN addgroup -g 111 app \
 && adduser -D -u 111 -G app app \
 && mkdir -p /app \
 && chown -R app:app /app

RUN apk update

# build dependencies
RUN apk add gcc musl-dev git && \
    pip install "poetry==$POETRY_VERSION"

# optional aiokafka dependency https://aiokafka.readthedocs.io/en/stable/#optional-snappy-install
RUN apk add snappy-dev

WORKDIR /app

COPY --chown=app:app poetry.lock pyproject.toml /app/
RUN poetry config settings.virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi

COPY --chown=app:app . /app
# https://github.com/sdispater/poetry/issues/668
# RUN poetry install --no-dev --no-interaction --no-ansi --extras core
RUN pip install -e ./jticker-core/

USER app

ENTRYPOINT ["python", "-m", "jitcker_controller"]
