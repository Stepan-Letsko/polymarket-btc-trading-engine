"""
Shared reconnect-with-silence-detection wrapper for the exchange feed
functions in this folder (stream_binance, stream_rtds,
stream_current_market, etc.). Used by the recorder/ scripts.

Any of these connections can drop (network blip, VM hiccup) — most
failures raise an exception, which this catches and retries after a
short pause. But a connection can also go silently stale: still "open"
from our side, receiving nothing, without ever raising anything —
observed for real with the RTDS feed specifically, which connected
successfully and then simply stopped producing messages for close to
two hours, with no error at all. To catch that too, this tracks when
the last message arrived and forces a reconnect if it's been quiet
for too long.
"""

import asyncio
import time

SILENCE_TIMEOUT = 30.0  # seconds with no message before treating a feed as stuck


async def run_forever(stream_fn, on_message, name="feed", silence_timeout=SILENCE_TIMEOUT):
    while True:
        last_message_at = time.monotonic()

        def wrapped_on_message(msg):
            nonlocal last_message_at
            last_message_at = time.monotonic()
            on_message(msg)

        stream_task = asyncio.create_task(stream_fn(on_message=wrapped_on_message))
        try:
            while True:
                done, _ = await asyncio.wait({stream_task}, timeout=5)
                if stream_task in done:
                    stream_task.result()  # re-raises whatever killed it
                    break
                if time.monotonic() - last_message_at > silence_timeout:
                    print(f"{name} feed silent for {silence_timeout:.0f}s with no messages, forcing reconnect...")
                    stream_task.cancel()
                    break
        except Exception as exc:
            print(f"{name} feed dropped ({exc!r}), reconnecting in 2s...")
        finally:
            if not stream_task.done():
                stream_task.cancel()
            try:
                await stream_task
            except (asyncio.CancelledError, Exception):
                pass
        await asyncio.sleep(2)
