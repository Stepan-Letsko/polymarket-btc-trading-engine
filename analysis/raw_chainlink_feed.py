"""
Prints the exact raw JSON Polymarket's RTDS sends for the Chainlink
BTC/USD feed — no parsing, no derived fields (recv_ts, relay_delay_ms,
total_latency_ms are all things WE compute in rtds_ws.py's
parse_message(); none of that exists in what Polymarket actually sends).
Reuses the real connection/subscription/heartbeat logic from rtds_ws.py
so this behaves identically to the real feed, just skips parsing.

Usage:
    python3 raw_chainlink_feed.py
"""

import asyncio
import json
import os
import sys

import websockets

ENGINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "trading_engine"))
sys.path.insert(0, ENGINE_DIR)

from rtds_ws import RTDS_URL, build_chainlink_subscription, heartbeat  # noqa: E402


async def main():
    print(f"Connecting to {RTDS_URL} ...")
    async with websockets.connect(RTDS_URL) as ws:
        await ws.send(build_chainlink_subscription())
        print("Subscribed. Streaming raw messages...\n")
        hb_task = asyncio.create_task(heartbeat(ws))
        try:
            async for raw in ws:
                if raw == "PONG":
                    continue
                if not raw:
                    # Empty frame - nothing to parse, skip it.
                    continue
                # Pretty-print exactly what was received, nothing added,
                # nothing removed.
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[non-JSON message] {raw!r}")
                    continue
                # The subscription sends every coin Polymarket tracks on
                # this topic (xrp, doge, eth, etc) — we only care about BTC.
                if data.get("payload", {}).get("symbol") != "btc/usd":
                    continue
                print(json.dumps(data, indent=2))
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
