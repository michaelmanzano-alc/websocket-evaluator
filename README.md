# WebSocket Block Race

A small benchmark that races multiple RPC providers over WebSocket and measures
who delivers each new block first. It supports Ethereum (`newHeads`), Solana
(`slotSubscribe`), and Bitcoin (new-block subscription).

## Setup

Create a virtual environment and install the one dependency:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install websockets
```

## Configure providers

Open `ws-script.py` and fill in the endpoint dicts near the top of the file
(`ETH_ENDPOINTS`, `SOLANA_ENDPOINTS`, `BTC_ENDPOINTS`). Each entry maps a
provider name to its full `wss://` URL, including the auth token. The name you
use as the key is the label that shows up in the results table. Entries left
as empty strings are skipped, and you can add, remove, or rename providers as
you like.

```python
ETH_ENDPOINTS = {
    "alchemy": "wss://eth-mainnet.g.alchemy.com/v2/<key>",
    "quicknode": "wss://<name>.ethereum-mainnet.quiknode.pro/<token>",
    "blockdaemon": "",   # empty, so it gets skipped
}
```

You need at least 2 filled-in endpoints per network to run a race. Since the
URLs contain your API keys, be careful not to commit this file to a repo with
real values in it.

## Run

```bash
python3 ws-script.py                            # Ethereum, 100 blocks (about 20 min)
python3 ws-script.py --mode solana --blocks 250 # Solana slots arrive ~2.5/s, finishes fast
python3 ws-script.py --mode btc --blocks 20     # Bitcoin averages ~10 min per block
python3 ws-script.py --timeout 600              # stop after 10 min and report what was collected
```

While it runs, a progress counter and any disconnect/reconnect events print to
stderr. The results table prints at the end, or you can Ctrl+C to stop early.

## How the benchmark works

- The script opens one WebSocket connection per provider and subscribes each
  one to new blocks (or slots).
- Every time a block notification arrives, it records the arrival timestamp
  for that provider and block ID.
- The first provider to deliver a given block wins that round. Every other
  provider's delay is measured relative to that first arrival, in milliseconds.
- Only complete rounds count toward the stats, meaning blocks that every
  provider reported. This keeps a flaky or slow-to-connect endpoint from
  skewing the numbers.
- If a connection drops, the script reconnects automatically after 1 second.
  Drops are counted per endpoint and shown in the `drops` column of the report.
- The run ends after the target number of complete rounds, or when the
  optional timeout hits. The report shows wins, win rate, and the average,
  P50, P90, and P99 delay behind the leader for each endpoint. The winner's
  delay is 0 by definition, so the percentiles read as how far behind the
  fastest provider each endpoint typically is.
