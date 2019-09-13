# Jticker Controller

Jticker management app.

# Usage
``` bash
python -m jticker_controller
```
## Help for arguments
```
usage: jticker_controller [-h] [--sentry-dsn SENTRY_DSN]
                          [--log-level LOG_LEVEL]
                          [--stats-log-interval STATS_LOG_INTERVAL]
                          [--web-host WEB_HOST] [--web-port WEB_PORT]
                          [--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS]
                          [--kafka-tasks-topic KAFKA_TASKS_TOPIC]

optional arguments:
  -h, --help            show this help message and exit
  --sentry-dsn SENTRY_DSN
                        Sentry DSN [default: None]
  --log-level LOG_LEVEL
                        Python logging level [default: INFO]
  --stats-log-interval STATS_LOG_INTERVAL
                        Stats logging interval [default: 60]
  --web-host WEB_HOST   Web server host [default: 0.0.0.0]
  --web-port WEB_PORT   Web server port [default: 8080]
  --kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS
                        Comma separated kafka bootstrap servers [default:
                        kafka:9092]
  --kafka-tasks-topic KAFKA_TASKS_TOPIC
                        Tasks kafka topic [default: grabber_tasks]
```
Any cli argument can be overriden by environment variable with matching name:
- `--sentry-dsn` → `SENTRY_DSN`
- `--kafka-bootstrap-servers` → `KAFKA_BOOTSTRAP_SERVERS`

and so on.

# Endpoints
- `/task/add` (mimics jticker-core [Task](https://github.com/jibrelnetwork/jticker-core/blob/master/jticker_core/task.py#L22) and its defaults)
    - `provider`: `str`
    - `symbols`: `array` of `str`
    - `from_dt`: (optional) `int` utc-timestamp
    - `to_dt`: (optional) `int` utc-timestamp
    - `type`: (optional) `simple` or `stream`
    - `interval`: (optional) `int` seconds
