import asyncio
import argparse
import json
import pickle
import zipfile

from aiohttp import ClientSession
from loguru import logger
from tqdm import tqdm

from jticker_core import Rate, TqdmLogFile


async def get_topic_data(session, base_url, topic):
    while True:
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


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="8080")
    parser.add_argument("--storage-file", default="storage.zip")
    ns = parser.parse_args()
    base_url = f"http://{ns.host}:{ns.port}"
    rate = Rate(log_period=15, log_template="receive candles rate: {:.3f} candles/s")
    with zipfile.ZipFile(ns.storage_file, mode="a", compression=zipfile.ZIP_DEFLATED) as zfile:
        async with ClientSession() as session:
            async with session.get(f"{base_url}/list_topics") as response:
                topics = await response.json()
            names = {n[:-len(".jsonl")] for n in zfile.namelist() if n.endswith(".jsonl")}
            for topic in tqdm(sorted(topics), ncols=80, ascii=True, mininterval=10,
                              maxinterval=10, file=TqdmLogFile(logger=logger)):
                if topic in names:
                    continue
                data = await get_topic_data(session, base_url, topic)
                with zfile.open(f"{topic}.jsonl", "w") as f:
                    for record in data:
                        line = json.dumps(record) + "\n"
                        f.write(line.encode())
                        rate.inc()


if __name__ == "__main__":
    asyncio.run(main())
