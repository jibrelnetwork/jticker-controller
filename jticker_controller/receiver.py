import asyncio
import argparse
import json
import pickle
import zipfile
import fnmatch

import backoff
from aiohttp import ClientSession, ClientConnectionError
from loguru import logger
from tqdm import tqdm

from jticker_core import Rate, TqdmLogFile, normalize_kafka_topic


@backoff.on_exception(
    backoff.constant,
    (ClientConnectionError,),
    jitter=None,
    interval=1)
async def get_topic_data(session, base_url, topic):
    while True:
        rate = Rate(log_period=15, log_template=topic + " candles rate: {:.3f} candles/s")
        records = []
        async with session.ws_connect(f"{base_url}/ws/get_candles") as ws:
            await ws.send_json(topic)
            async for msg in ws:
                if not msg.data:
                    return records
                for record in pickle.loads(msg.data):
                    d = record._asdict()
                    d["key"] = d["key"].decode()
                    d["value"] = json.loads(d["value"])
                    records.append(d)
                    rate.inc()


GRABBED_HISTORICAL_EXCHANGES = {"binance", "bitfinex", "hitbtc"}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", default="http://localhost:8080")
    parser.add_argument("--storage-file", default="storage.zip")
    parser.add_argument("--topic-mask", default="*")
    parser.add_argument("--allow-all-exchanges", default=False, action="store_true")
    ns = parser.parse_args()
    base_url = ns.url.rstrip("/")
    rate = Rate(log_period=15, log_template="global candles rate: {:.3f} candles/s")
    with zipfile.ZipFile(ns.storage_file, mode="a", compression=zipfile.ZIP_DEFLATED) as zfile:
        async with ClientSession() as session:
            async with session.get(f"{base_url}/list_topics") as response:
                topics = set(map(normalize_kafka_topic, await response.json()))
            names = {n[:-len(".jsonl")] for n in zfile.namelist() if n.endswith(".jsonl")}
            topics -= names
            topics = fnmatch.filter(sorted(topics), ns.topic_mask)
            for topic in tqdm(topics, ncols=80, ascii=True, mininterval=10,
                              maxinterval=10, file=TqdmLogFile(logger=logger)):

                if not ns.allow_all_exchanges and \
                        any(topic.startswith(prefix) for prefix in GRABBED_HISTORICAL_EXCHANGES):
                    continue
                data = await get_topic_data(session, base_url, topic)
                if not data:
                    continue
                with zfile.open(f"{topic}.jsonl", "w") as f:
                    for record in data:
                        line = json.dumps(record) + "\n"
                        f.write(line.encode())
                        rate.inc()


if __name__ == "__main__":
    asyncio.run(main())
