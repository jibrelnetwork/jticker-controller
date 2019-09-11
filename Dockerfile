FROM python:3.7-alpine

ENV KAFKA_BOOTSTRAP_SERVERS "kafka:9092"
ENV LOG_LEVEL "INFO"
ENV SENTRY_DSN ""

RUN addgroup -g 111 app \
 && adduser -D -u 111 -G app app \
 && mkdir -p /app \
 && chown -R app:app /app

# optional aiokafka dependency https://aiokafka.readthedocs.io/en/stable/#optional-snappy-install
# RUN apk update && apk add snappy-dev

WORKDIR /app

COPY --chown=app:app requirements.txt /app/
RUN pip install -r requirements.txt

COPY --chown=app:app . /app
RUN pip install -e ./jticker-core -e ./

USER app

ENTRYPOINT ["python", "-m", "jitcker_controller"]
