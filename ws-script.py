#!/usr/bin/env python3
"""
ws-script.py: WebSocket block-propagation benchmark for RPC providers.

Races multiple RPC endpoints against each other on:
  * Ethereum  `eth_subscribe("newHeads")`   (new block headers)
  * Solana    `slotSubscribe`               (new slots)
  * Bitcoin   new-block subscription        (Blockbook-compatible by default)

For every block/slot, the first endpoint to deliver the payload "wins" the
round (delta = 0). Every other endpoint's delta is measured relative to that
first payload. At the end you get, per endpoint:

  * number of wins
  * P50 / P90 / P99 of the delta-from-first (ms)
  * average delay from first (ms)
  * number of disconnects (each one triggers an automatic reconnect)

Only rounds in which *every* endpoint reported the block are used for the
percentile/average stats, so slow-to-connect endpoints don't skew results.

Dependencies: only `websockets` (pip install websockets). Everything else
is stdlib.

Usage:
    python ws-script.py                 # default: eth, 100 blocks
    python ws-script.py --mode solana --blocks 250
    python ws-script.py --mode btc --timeout 7200
"""

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections import defaultdict

try:
    import websockets
except ImportError:
    sys.exit("Missing dependency: pip install websockets")

# ---------------------------------------------------------------------------
# Endpoints: provider name -> full wss:// URL (auth token included).
# Fill in the URLs below; entries left empty are ignored. Add/remove/rename
# providers freely — the key is the label shown in the results table.
# Racing needs >= 2 filled-in endpoints per network.
#
# WARNING: don't commit this file with real keys/tokens in it.
# ---------------------------------------------------------------------------
ETH_ENDPOINTS = {
    "alchemy": "",     # wss://eth-mainnet.g.alchemy.com/v2/<key>
    "quicknode": "",   # wss://<name>.ethereum-mainnet.quiknode.pro/<token>
    "blockdaemon": "",
}

SOLANA_ENDPOINTS = {
    "alchemy": "",     # wss://solana-mainnet.g.alchemy.com/v2/<key>
    "quicknode": "",   # wss://<name>.solana-mainnet.quiknode.pro/<token>/
    "blockdaemon": "",
}

BTC_ENDPOINTS = {
    # Check your app's "Network" tab in the Alchemy dashboard for the exact
    # websocket URL for Bitcoin (HTTP is bitcoin-mainnet.alchemy-blast.com).
    "alchemy": "",     # wss://bitcoin-mainnet.alchemy-blast.com/v2/<key>
    "blockdaemon": "",
}

# ---------------------------------------------------------------------------
# Subscription payloads.
#
# BTC note: Alchemy's UTXO websockets support new-block subscriptions with
# Blockbook (bb_*) compatible signatures. If your endpoint expects a different
# method name, this dict is the only thing you need to change — the notification
# parser below already handles both Blockbook-style ({"data": {"height", "hash"}})
# and JSON-RPC-style ({"params": {"result": {...}}}) block messages.
# ---------------------------------------------------------------------------
SUBSCRIBE_PAYLOADS = {
    "eth": {
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_subscribe", "params": ["newHeads"],
    },
    "solana": {
        "jsonrpc": "2.0", "id": 1,
        "method": "slotSubscribe", "params": [],
    },
    "btc": {
        "jsonrpc": "2.0", "id": 1,
        "method": "subscribeNewBlock", "params": {},
    },
}


def _to_int(value):
    """Accept ints, decimal strings, and 0x-hex strings."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return None
    return None


def extract_block_id(mode: str, msg: dict):
    """Pull a comparable block identifier out of a notification.

    Returns an int (block number / slot / height), a string (block hash, as a
    last resort for BTC), or None if the message isn't a block notification.
    """
    if mode == "eth":
        result = (msg.get("params") or {}).get("result")
        if isinstance(result, dict):
            return _to_int(result.get("number"))
        return None

    if mode == "solana":
        result = (msg.get("params") or {}).get("result")
        if isinstance(result, dict):
            return _to_int(result.get("slot"))
        return None

    # btc — tolerate the common notification shapes:
    #   Blockbook style:  {"id": "...", "data": {"height": 900001, "hash": "..."}}
    #   JSON-RPC style:   {"method": "...", "params": {"result": {"height": ...}}}
    for candidate in (
        msg.get("data"),
        (msg.get("params") or {}).get("result") if isinstance(msg.get("params"), dict) else None,
        msg.get("result") if isinstance(msg.get("result"), dict) else None,
    ):
        if isinstance(candidate, dict):
            height = _to_int(candidate.get("height") or candidate.get("blockHeight"))
            if height is not None:
                return height
            block_hash = candidate.get("hash") or candidate.get("blockHash")
            if block_hash:
                return block_hash  # hashes are still race-able keys
    return None


class Race:
    """Collects per-block arrival timestamps from all endpoints."""

    def __init__(self, providers, target_blocks):
        self.providers = list(providers)
        self.target = target_blocks
        # block_id -> {provider: monotonic_ts}
        self.arrivals: dict = defaultdict(dict)
        self.completed: set = set()   # blocks seen by ALL endpoints
        self.disconnects = defaultdict(int)  # provider -> reconnect count
        self.done = asyncio.Event()

    def record(self, provider: str, block_id):
        seen = self.arrivals[block_id]
        if provider in seen:
            return  # duplicate notification
        seen[provider] = time.monotonic()
        if len(seen) == len(self.providers):
            self.completed.add(block_id)
            n = len(self.completed)
            sys.stderr.write(f"\rcomplete rounds: {n}/{self.target}   ")
            sys.stderr.flush()
            if n >= self.target:
                self.done.set()


RECONNECT_DELAY_S = 1.0


async def listen(name: str, url: str, mode: str, race: Race):
    """Connect, subscribe, and feed notifications into the race.

    Reconnects (with a short delay) whenever the connection drops, counting
    each drop in race.disconnects. RPC-level errors (e.g. bad key) are
    terminal — reconnecting won't fix those.
    """
    while not race.done.is_set():
        try:
            async with websockets.connect(url, max_size=2**24, ping_interval=20) as ws:
                await ws.send(json.dumps(SUBSCRIBE_PAYLOADS[mode]))
                async for raw in ws:
                    if race.done.is_set():
                        return
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(msg, dict) and "error" in msg and msg["error"]:
                        print(f"\n[{name}] RPC error: {msg['error']}", file=sys.stderr)
                        return
                    if not isinstance(msg, dict):
                        continue
                    block_id = extract_block_id(mode, msg)
                    if block_id is not None:
                        race.record(name, block_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if race.done.is_set():
                return
            race.disconnects[name] += 1
            print(f"\n[{name}] disconnected (#{race.disconnects[name]}): {e} "
                  f"— reconnecting in {RECONNECT_DELAY_S:g}s", file=sys.stderr)
            await asyncio.sleep(RECONNECT_DELAY_S)
            continue
        # Server closed the stream cleanly (async for ended without error).
        if race.done.is_set():
            return
        race.disconnects[name] += 1
        print(f"\n[{name}] connection closed (#{race.disconnects[name]}) "
              f"— reconnecting in {RECONNECT_DELAY_S:g}s", file=sys.stderr)
        await asyncio.sleep(RECONNECT_DELAY_S)


def percentile(sorted_vals, p):
    """Nearest-rank percentile on a pre-sorted list."""
    if not sorted_vals:
        return float("nan")
    k = max(0, min(len(sorted_vals) - 1, round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[k]


def report(race: Race):
    wins = defaultdict(int)
    deltas_ms = defaultdict(list)  # provider -> [delta from first, ms]

    for block_id in race.completed:
        arrivals = race.arrivals[block_id]
        first_ts = min(arrivals.values())
        winner = min(arrivals, key=arrivals.get)
        wins[winner] += 1
        for provider, ts in arrivals.items():
            deltas_ms[provider].append((ts - first_ts) * 1000.0)

    total = len(race.completed)
    print(f"\n\nResults over {total} complete rounds "
          f"(blocks seen by all {len(race.providers)} endpoints)\n")
    header = (f"{'endpoint':<16}{'wins':>6}{'win %':>8}"
              f"{'avg ms':>10}{'p50 ms':>10}{'p90 ms':>10}{'p99 ms':>10}"
              f"{'drops':>7}")
    print(header)
    print("-" * len(header))

    for provider in sorted(race.providers, key=lambda p: -wins[p]):
        d = sorted(deltas_ms[provider])
        print(f"{provider:<16}"
              f"{wins[provider]:>6}"
              f"{(100 * wins[provider] / total if total else 0):>7.1f}%"
              f"{(statistics.fmean(d) if d else float('nan')):>10.1f}"
              f"{percentile(d, 50):>10.1f}"
              f"{percentile(d, 90):>10.1f}"
              f"{percentile(d, 99):>10.1f}"
              f"{race.disconnects[provider]:>7}")

    print("\nNote: deltas are relative to the first payload per block, so the")
    print("winner's delta is 0 by definition; avg/percentiles reflect how far")
    print("behind the leader each endpoint typically is.")


async def main():
    ap = argparse.ArgumentParser(description="WebSocket block-propagation race")
    ap.add_argument("--mode", choices=["eth", "solana", "btc"], default="eth",
                    help="eth newHeads, solana slots, or btc new blocks (default: eth)")
    ap.add_argument("--blocks", type=int, default=100,
                    help="number of complete rounds to measure (default: 100)")
    ap.add_argument("--timeout", type=float, default=None,
                    help="optional overall timeout in seconds")
    args = ap.parse_args()

    endpoints = {
        "eth": ETH_ENDPOINTS,
        "solana": SOLANA_ENDPOINTS,
        "btc": BTC_ENDPOINTS,
    }[args.mode]
    endpoints = {name: url for name, url in endpoints.items() if url}
    if len(endpoints) < 2:
        sys.exit(f"Need at least 2 endpoints to race (found {len(endpoints)} for "
                 f"{args.mode}). Fill in the {args.mode.upper()}_ENDPOINTS URLs "
                 "at the top of this script.")

    if args.mode == "btc" and args.timeout is None:
        # ~10 min/block means 100 blocks ≈ 17 hours; warn so nobody is surprised.
        print("Heads up: Bitcoin averages one block every ~10 minutes, so "
              f"{args.blocks} blocks will take roughly {args.blocks * 10 / 60:.0f} "
              "hours. Consider --blocks 20 or a --timeout.", file=sys.stderr)

    race = Race(endpoints.keys(), args.blocks)
    tasks = [asyncio.create_task(listen(n, u, args.mode, race))
             for n, u in endpoints.items()]

    print(f"Racing {len(endpoints)} endpoints on {args.mode} "
          f"for {args.blocks} blocks...", file=sys.stderr)

    try:
        await asyncio.wait_for(race.done.wait(), timeout=args.timeout)
    except asyncio.TimeoutError:
        print("\nTimeout reached; reporting on rounds collected so far.",
              file=sys.stderr)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if not race.completed:
        sys.exit("No complete rounds collected — check your endpoints/keys.")
    report(race)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
