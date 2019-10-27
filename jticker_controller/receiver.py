import asyncio
import argparse
import json
import pickle
import zipfile
from pathlib import Path

from aiohttp import ClientSession

from jticker_core import Rate


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="8080")
    parser.add_argument("--storage-file", default="storage.zip")
    ns = parser.parse_args()
    rate = Rate(log_period=15, log_template="receive candles rate: {:.3f} candles/s")
    with zipfile.ZipFile(ns.storage_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zfile:
        last_topic = fd = None
        async with ClientSession() as session:
            async with session.ws_connect(f"http://{ns.host}:{ns.port}/ws/get_candles") as ws:
                async for msg in ws:
                    chunk = pickle.loads(msg.data)
                    topic = chunk[0].topic
                    if topic != last_topic:
                        if fd:
                            fd.close()
                        fd = zfile.open(f"{topic}.jsonl", mode="w")
                        last_topic = topic
                    for record in chunk:
                        d = record._asdict()
                        d["key"] = d["key"].decode()
                        d["value"] = json.loads(d["value"])
                        line = json.dumps(d) + "\n"
                        fd.write(line.encode("utf-8"))
                        rate.inc()
                    fd.flush()
                if fd:
                    fd.close()


if __name__ == "__main__":
    asyncio.run(main())
