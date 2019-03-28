FROM python:3.7-alpine

RUN addgroup -g 111 app \
 && adduser -D -u 111 -G app app \
 && mkdir -p /app \
 && chown -R app:app /app

RUN apk update

# build dependencies
RUN apk add gcc musl-dev

WORKDIR /app

COPY --chown=app:app requirements.txt /app/
RUN pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app . /app

USER app

ENTRYPOINT ["/app/run.sh"]
