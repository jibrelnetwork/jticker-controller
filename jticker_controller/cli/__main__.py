import argparse
import traceback

import requests
from tabulate import tabulate


def _do_request(url, q):
    response = requests.get(url, params=dict(query=q))
    try:
        response.raise_for_status()
        data = response.json()
    except Exception:
        traceback.print_exc()
    else:
        if data["status"] == "error":
            print("error:", data["exception"])
            return
        for series, inf_data in enumerate(data["result"]):
            if not inf_data:
                continue
            print(f"series #{series}:")
            print(tabulate(inf_data, headers="keys"))


def main():
    parser = argparse.ArgumentParser("cli")
    parser.add_argument("controller", help="Controller url address")
    ns = parser.parse_args()
    url = f"{ns.controller}/storage/query"
    print(f"Simple storage query via remote controller ({url})")
    while True:
        try:
            q = input("> ")
        except KeyboardInterrupt:
            continue
        except EOFError:
            print()
            return

        q = q.strip()
        if not q:
            continue

        _do_request(url, q)


if __name__ == "__main__":
    main()
