FROM python:3.7-alpine as builder

RUN apk update && apk add gcc musl-dev git
COPY jticker-core/requirements.txt /requirements-core.txt
COPY requirements.txt /
RUN pip install --no-cache-dir -r requirements-core.txt -r requirements.txt

FROM python:3.7-alpine as runner

COPY --from=builder /usr/local/lib/python3.7/site-packages/ /usr/local/lib/python3.7/site-packages/

RUN addgroup -g 111 app \
 && adduser -D -u 111 -G app app \
 && mkdir -p /app \
 && chown -R app:app /app

WORKDIR /app

COPY --chown=app:app . /app
RUN pip install --no-cache-dir -e ./jticker-core -e ./

USER app

ENTRYPOINT ["python", "-m", "jticker_controller"]
