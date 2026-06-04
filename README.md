# Slimcoin Explorer

A small self-hosted Slimcoin blockchain explorer based on Flask, Gunicorn, Caddy and SQLite.

The recommended setup is to start from a downloadable prebuilt SQLite database (`blockchain.db` and optionally `peers.db`) and then keep it up to date with the Slimcoin node via RPC.

---

## 1. Overview

The explorer consists of three parts:

| Component | Purpose |
|---|---|
| `app.py` | Flask web application / explorer frontend |
| `database.py` | SQLite schema, RPC indexing helpers, peer updates |
| `update_blocks.py` | Continuous updater loop for new blocks and peers |
| `reindex.py` | Optional full or partial reindex from the Slimcoin node |
| `blockchain.db` | Main SQLite explorer database |
| `peers.db` | Small SQLite database for network peers |

The usual production stack is:

```text
Browser
  ↓ HTTPS
Caddy
  ↓ reverse proxy
Gunicorn
  ↓ WSGI
Flask app.py
  ↓ read-only SQLite
blockchain.db / peers.db
```

`update_blocks.py` runs separately and periodically updates the database from a local Slimcoin node.

---

## 2. Requirements

Recommended system:

- Linux server or VPS
- Python 3.10+
- Slimcoin node with RPC enabled
- `git`
- `python3-venv`
- `sqlite3`
- Optional but recommended: `caddy`, `systemd`

Install base packages on Debian/Ubuntu:

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip sqlite3
```

For public HTTPS hosting:

```bash
sudo apt install -y caddy
```

---

## 3. Clone the repository

```bash
cd ~
git clone https://github.com/YOUR_USER/Slimcoin-Explorer.git
cd Slimcoin-Explorer
```

Replace the repository URL with the actual project URL.

---

## 4. Create the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Typical `requirements.txt`:

```text
Flask~=3.0.3
Flask-Caching~=2.3
python-dotenv~=1.0.1
requests~=2.32.2
```

If `dotenv~=0.9.9` is still present, it is usually not needed when `python-dotenv` is installed.

---

## 5. Configure Slimcoin RPC

Create a `.env` file in the explorer directory:

```bash
nano .env
```

Example:

```env
RPC_USER=your_rpc_user
RPC_PASSWORD=your_rpc_password
RPC_HOST=127.0.0.1
RPC_PORT=41683
RPC_PREFIX=http

INDEX_BATCH_SIZE=1
INDEX_COMMIT_INTERVAL=1
TX_BATCH_CHUNK=200
GETBLOCK_VERBOSE=2
PREFETCH_WORKERS=2
PREFETCH_DEPTH=1

CONTINUE_REWIND=10
```

Your Slimcoin node must allow RPC connections from the explorer host.

A typical `slimcoin.conf` contains something like:

```ini
server=1
rpcuser=your_rpc_user
rpcpassword=your_rpc_password
rpcallowip=127.0.0.1
rpcport=41683
```

Restart the Slimcoin daemon after changing RPC settings.

---

## 6. Download the prebuilt databases

The recommended setup uses downloadable database snapshots.
The zipped blockchain database can be downloaded from https://slimcoin-project.github.io/

Place the unzipped `blockchain.db` file in the project directory:

```text
Slimcoin-Explorer/
├── blockchain.db
├── peers.db              optional
├── app.py
├── database.py
├── update_blocks.py
└── ...
```

If `peers.db` is missing, it will be created automatically by the updater.

Make sure the service user owns the files:

```bash
sudo chown -R $USER:$USER ~/Slimcoin-Explorer
```

Basic checks:

```bash
sqlite3 blockchain.db "SELECT MAX(block_height) FROM blocks;"
sqlite3 blockchain.db "SELECT COUNT(*) FROM transactions;"
sqlite3 blockchain.db "SELECT value FROM explorer_meta WHERE key='total_supply_fast';"
```

---

## 7. Run the explorer locally

Test the Flask/Gunicorn stack manually:

```bash
source .venv/bin/activate
gunicorn -w 2 -k gthread --threads 4 --timeout 120 --access-logfile - --error-logfile - -b 127.0.0.1:5005 app:app
```

Then open locally:

```text
http://127.0.0.1:5005/
```

If testing from another machine in the LAN, bind to all interfaces temporarily:

```bash
gunicorn -w 2 -k gthread --threads 4 --timeout 120 -b 0.0.0.0:5005 app:app
```

Then open:

```text
http://SERVER_LAN_IP:5005/
```

For production, keep Gunicorn bound to `127.0.0.1:5005` and expose it through Caddy.

---

## 8. Run `app.py` via systemd

Create the service:

```bash
sudo nano /etc/systemd/system/slimcoin-explorer.service
```

Example for user `j4005`:

```ini
[Unit]
Description=Slimcoin Explorer (Flask via Gunicorn)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=j4005
Group=j4005
WorkingDirectory=/home/j4005/Slimcoin-Explorer
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/home/j4005/Slimcoin-Explorer/.env
ExecStart=/home/j4005/Slimcoin-Explorer/.venv/bin/gunicorn \
  -w 2 -k gthread --threads 4 --timeout 120 --keep-alive 5 \
  --access-logfile - --error-logfile - \
  -b 127.0.0.1:5005 app:app
Restart=always
RestartSec=3
LimitNOFILE=4096

[Install]
WantedBy=multi-user.target
```

For a small 2-core machine use:

```ini
-w 2 -k gthread --threads 4
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now slimcoin-explorer
sudo systemctl status slimcoin-explorer
```

Logs:

```bash
journalctl -u slimcoin-explorer -f
```

---

## 9. Configure Caddy reverse proxy

Create or edit:

```bash
sudo nano /etc/caddy/Caddyfile
```

Example:

```caddy
{
    email you@example.com
    acme_ca https://acme-v02.api.letsencrypt.org/directory
    cert_issuer acme
}

slimcoinexplorer.example.org {
    encode zstd gzip
    reverse_proxy 127.0.0.1:5005
}
```

Validate and reload:

```bash
sudo caddy fmt --overwrite /etc/caddy/Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

Check:

```bash
curl -I http://slimcoinexplorer.example.org
curl -I https://slimcoinexplorer.example.org
```

Your router/firewall must forward TCP ports `80` and `443` to the server running Caddy.

---

## 10. Keep the database updated

The simple updater is `update_blocks.py`:

```python
import time
from database import update_with_latest_block, update_peers

print("Updating database...")

while True:
    update_peers()
    update_with_latest_block()
    time.sleep(60)
```

It does two things:

1. Updates `peers.db` via `getpeerinfo`.
2. Adds new blocks to `blockchain.db` via RPC.

### Option A: Run in tmux

```bash
sudo apt install -y tmux
tmux new -s slimcoin-updater
cd ~/Slimcoin-Explorer
source .venv/bin/activate
python update_blocks.py
```

Detach from tmux:

```text
Ctrl+B, then D
```

Reattach later:

```bash
tmux attach -t slimcoin-updater
```

### Option B: Run via systemd

Create:

```bash
sudo nano /etc/systemd/system/slimcoin-updater.service
```

Example:

```ini
[Unit]
Description=Slimcoin Explorer database updater
After=network-online.target slimcoin-explorer.service
Wants=network-online.target

[Service]
Type=simple
User=j4005
Group=j4005
WorkingDirectory=/home/j4005/Slimcoin-Explorer
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/home/j4005/Slimcoin-Explorer/.env
ExecStart=/home/j4005/Slimcoin-Explorer/.venv/bin/python /home/j4005/Slimcoin-Explorer/update_blocks.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now slimcoin-updater
sudo systemctl status slimcoin-updater
```

Logs:

```bash
journalctl -u slimcoin-updater -f
```

---

## 11. Reindexing from RPC

Normally users should start from a downloadable `blockchain.db`.

If needed, the database can also be built from the Slimcoin node using `reindex.py`.

```python
#!/usr/bin/env python3
from database import reindex_db, reindex_db_continue, CONTINUE_REWIND

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Slimcoin explorer indexer utilities')
    parser.add_argument('--reindex', action='store_true',
                        help='Full reindex (drops tables). Combine with --height to reindex from a given height without dropping earlier data.')
    parser.add_argument('--continue', dest='cont', action='store_true',
                        help='Continue indexing to tip, rewinding a few blocks first to be safe.')
    parser.add_argument('--height', type=int, default=None,
                        help='Start height for partial reindex (used with --reindex).')
    parser.add_argument('--rewind', type=int, default=None,
                        help='Override number of blocks to rewind for --continue (default CONTINUE_REWIND or 10).')

    args = parser.parse_args()

    if args.cont:
        rw = args.rewind if args.rewind is not None else CONTINUE_REWIND
        reindex_db_continue(rewind=rw)
    elif args.reindex:
        if args.height and args.height > 0:
            reindex_db(start_height=args.height)
        else:
            reindex_db()
    else:
        parser.print_help()
```

### Full reindex

This drops and recreates the main tables:

```bash
source .venv/bin/activate
python reindex.py --reindex
```

This can take a long time.

### Continue after interruption

If indexing was interrupted, continue with a small rewind:

```bash
python reindex.py --continue
```

Override rewind:

```bash
python reindex.py --continue --rewind 25
```

### Partial reindex from a height

```bash
python reindex.py --reindex --height 4000000
```

This purges data from the given height and rebuilds from there to the current tip.

---

## 12. Useful SQLite maintenance commands

Check current indexed height:

```bash
sqlite3 blockchain.db "SELECT MAX(block_height) FROM blocks;"
```

Check top addresses:

```bash
sqlite3 blockchain.db "
SELECT address, balance
FROM addresses
WHERE balance > 0
ORDER BY balance DESC
LIMIT 10;
"
```

Check cached circulating supply:

```bash
sqlite3 blockchain.db "
SELECT key, value, datetime(updated_at, 'unixepoch')
FROM explorer_meta
WHERE key='total_supply_fast';
"
```

Remove SQLite WAL/SHM only when the app and updater are stopped:

```bash
sudo systemctl stop slimcoin-explorer slimcoin-updater
ls -lh blockchain.db*
```

Do not delete `blockchain.db-wal` while services are writing.

---

## 13. Performance notes

The explorer is designed for small hardware.

Recommended settings:

| Hardware | Gunicorn |
|---|---|
| 2-core CPU | `-w 2 -k gthread --threads 4` |
| 4+ cores | `-w 2..4 -k gthread --threads 4..8` |

Important performance features:

- Start page loads only the latest blocks.
- Richlist uses the precomputed `addresses` table.
- `calculate_total_supply_fast()` reads cached supply from `explorer_meta`.
- Flask-Caching caches heavy routes.
- SQLite indices should include:
  - `idx_blocks_height`
  - `idx_tx_block`
  - `idx_vout_txn`
  - `idx_vout_addr`
  - `idx_vout_addr_spent`
  - `idx_vout_addr_unspent`
  - `idx_vout_blockhash`
  - `idx_vin_txid`
  - `idx_vin_outref`
  - `idx_addresses_balance_desc_addr`

If the first request after restart is slow, wait for cache warmup or reload once.

---

## 14. Troubleshooting

### The site loads locally but not publicly

Check Caddy:

```bash
sudo systemctl status caddy
sudo journalctl -u caddy -n 100 --no-pager
sudo ss -tlnp | grep -E '(:80|:443)'
```

Check DNS:

```bash
dig +short A slimcoinexplorer.example.org @1.1.1.1
curl -I http://slimcoinexplorer.example.org
```

### Caddy certificate fails

Usually one of these is wrong:

- DNS does not point to your current public IP.
- Port 80 is not forwarded to Caddy.
- Port 443 is used by the router itself.
- Another service already listens on 80/443.

### `UNIQUE constraint failed: transactions.txid`

This usually indicates a reorg or a partial interrupted update. The updater contains a small rewind/purge handler. If needed:

```bash
python reindex.py --continue --rewind 25
```

or:

```bash
python reindex.py --reindex --height HEIGHT
```

### Richlist or supply looks wrong

Rebuild address aggregates:

```bash
python reindex.py --continue --rewind 10
```

or, if using the blk importer:

```bash
python blk_importer_sqlite.py --post-only
```

Then restart the web service to clear Flask caches:

```bash
sudo systemctl restart slimcoin-explorer
```

### `Permission denied` with systemd

Check that the service user exists and owns the project:

```bash
id j4005
sudo chown -R j4005:j4005 /home/j4005/Slimcoin-Explorer
systemctl cat slimcoin-explorer
```

### `mode=ro` SQLite errors in app.py

Make sure `app.py` uses:

```python
uri = f"file:{DATABASE}?mode=ro&cache=shared"
```

not the HTML-escaped form:

```python
uri = f"file:{DATABASE}?mode=ro&amp;cache=shared"
```

---

## 15. Recommended production checklist

- [ ] Slimcoin node is fully synced.
- [ ] RPC works locally.
- [ ] `.env` contains correct RPC credentials.
- [ ] `blockchain.db` exists and contains recent blocks.
- [ ] `explorer_meta.total_supply_fast` exists.
- [ ] `slimcoin-explorer.service` is active.
- [ ] `slimcoin-updater.service` or tmux updater is running.
- [ ] Caddy is active.
- [ ] DNS points to the correct public IP.
- [ ] Router forwards ports 80 and 443 to the Caddy host.
- [ ] `/richlist`, `/blocks_frame`, `/stats`, `/network` load correctly.

---

## License

MIT license
