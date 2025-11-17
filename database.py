import sqlite3
import requests
import json
from dotenv import load_dotenv
import os
from decimal import Decimal
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

# Reusable HTTP session for RPC calls
from requests.adapters import HTTPAdapter

_session = requests.Session()
_session.headers.update({'content-type': 'application/json', 'Connection': 'keep-alive'})
# Increase connection pools to better reuse TCP sessions during batching
_session.mount('http://', HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0))
_session.mount('https://', HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0))


def _normalize_block_time(t):
    if isinstance(t, int):
        return t
    if isinstance(t, str):
        # Slimcoin returns e.g. "2014-05-28 21:13:23 UTC"
        ts = t.replace(' UTC', '')
        try:
            dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0
    return 0


load_dotenv()  # This loads the variables from .env into the environment

rpc_user = os.getenv("RPC_USER")
rpc_password = os.getenv("RPC_PASSWORD")
rpc_host = os.getenv("RPC_HOST")
rpc_port = os.getenv("RPC_PORT")
rpc_prefix = os.getenv("RPC_PREFIX")

BATCH_SIZE = int(os.getenv("INDEX_BATCH_SIZE", "100"))
# Configurable commit interval and TX batch chunk size
COMMIT_INTERVAL = int(os.getenv("INDEX_COMMIT_INTERVAL", "100"))
TX_BATCH_CHUNK = int(os.getenv("TX_BATCH_CHUNK", "200"))
GETBLOCK_VERBOSE = os.getenv("GETBLOCK_VERBOSE", "auto").lower()  # "auto", "0", "1", "2"
GETBLOCK_VERBOSE_MODE = None  # resolved to: 'ids_only' | 'txinfo' | 'txinfo_details'

# Prefetch pipeline config
PREFETCH_WORKERS = int(os.getenv("PREFETCH_WORKERS", "3"))  # parallel RPC workers for prefetch
PREFETCH_DEPTH = int(os.getenv("PREFETCH_DEPTH", "3"))      # how many batches to keep in flight

# Incremental update tunables
INCR_COMMIT_EVERY = int(os.getenv("INCR_COMMIT_EVERY", "1"))  # commit every N blocks during incremental updates
REINDEX_LOCK_STALE_SECS = int(os.getenv("REINDEX_LOCK_STALE_SECS", "7200"))  # treat lock older than 2h as stale
CONTINUE_REWIND = int(os.getenv("CONTINUE_REWIND", "10"))

DATABASE = 'blockchain.db'
PEERS = 'peers.db'
LOCK_FILE = '.reindex.lock'


def rpc_request(method, params=None):
    if params is None:
        params = []
    payload = {
        "jsonrpc": "1.0",
        "id": "curltest",
        "method": method,
        "params": params
    }
    try:
        _response = _session.post(
            f"{rpc_prefix}://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}",
            data=json.dumps(payload),
            timeout=60,
            verify=True
        )
        _response.raise_for_status()
        return _response.json()
    except requests.exceptions.RequestException as e:
        print(f"RPC call failed: {e}")
        return {"error": str(e)}


# --- Batch RPC helpers ---
def rpc_batch(method_params_list):
    """
    Send a JSON-RPC batch.
    method_params_list: list of tuples (method, params)
    Returns a list of response dicts in the same order as input.
    """
    calls = []
    for i, (m, p) in enumerate(method_params_list):
        calls.append({
            "jsonrpc": "1.0",
            "id": str(i),
            "method": m,
            "params": p or []
        })
    try:
        r = _session.post(
            f"{rpc_prefix}://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}",
            data=json.dumps(calls),
            timeout=120,
            verify=True
        )
        r.raise_for_status()
        resp = r.json()
        if not isinstance(resp, list):
            # Server does not support batch (or returned error)
            return []
        # Map by id so we can restore the input order
        by_id = {str(item.get("id")): item for item in resp}
        ordered = [by_id.get(str(i)) for i in range(len(calls))]
        return ordered
    except requests.exceptions.RequestException as e:
        print(f"RPC batch failed: {e}")
        return []


def batch_getblockhashes(heights):
    calls = [("getblockhash", [h]) for h in heights]
    results = rpc_batch(calls)
    out = []
    for item in results:
        if not item or item.get("error"):
            out.append(None)
        else:
            out.append(item.get("result"))
    return out


def batch_getblocks(hashes):
    calls = [("getblock", [h]) for h in hashes]
    results = rpc_batch(calls)
    out = []
    for item in results:
        if not item or item.get("error"):
            out.append(None)
        else:
            out.append(item.get("result"))
    return out


# --- getblock verbose auto helpers ---
def _block_has_decoded_txs(block_obj: dict) -> bool:
    if not isinstance(block_obj, dict):
        return False
    txs = block_obj.get('tx')
    return isinstance(txs, list) and len(txs) > 0 and isinstance(txs[0], dict) and 'vin' in txs[0] and 'vout' in txs[0]


def detect_getblock_verbose_mode():
    """
    Probes the node to detect whether getblock can return decoded tx objects.
    Returns one of: 'ids_only', 'txinfo', 'txinfo_details'
    """
    try:
        tip = int(getblockcount() or 0)
        probe_h = max(1, tip // 2)
        h = get_block_hash(probe_h) or get_block_hash(1)
        if not h:
            return 'ids_only'

        r1 = rpc_request('getblock', [h, True])
        if r1.get('error') is None and isinstance(r1.get('result'), dict):
            if _block_has_decoded_txs(r1['result']):
                return 'txinfo'

        r2 = rpc_request('getblock', [h, True, True])
        if r2.get('error') is None and isinstance(r2.get('result'), dict):
            if _block_has_decoded_txs(r2['result']):
                return 'txinfo_details'
    except Exception:
        pass
    return 'ids_only'


def batch_getblocks_verbose_auto(hashes, mode: str):
    """
    Batch getblock with verbosity based on detected 'mode'.
    mode: 'ids_only'        => getblock(hash)
          'txinfo'          => getblock(hash, True)
          'txinfo_details'  => getblock(hash, True, True)
    Returns a list of block objects (or None on failure) in the same order as 'hashes'.
    """
    if mode == 'txinfo_details':
        calls = [("getblock", [h, True, True]) for h in hashes]
    elif mode == 'txinfo':
        calls = [("getblock", [h, True]) for h in hashes]
    else:
        calls = [("getblock", [h]) for h in hashes]
    results = rpc_batch(calls)
    out = []
    for item in results:
        if not item or item.get("error"):
            out.append(None)
        else:
            out.append(item.get("result"))
    return out


# --- Helper to resolve getblock verbose mode without mutating a global ---
def resolve_verbose_mode():
    """
    Compute the desired getblock verbosity without using or mutating globals.
    """
    m = (GETBLOCK_VERBOSE or "auto").strip().lower()
    if m in ("auto", ""):
        return detect_getblock_verbose_mode()
    mapping = {"0": "ids_only", "false": "ids_only", "1": "txinfo", "true": "txinfo", "2": "txinfo_details"}
    return mapping.get(m, "ids_only")


# --- Prefetch pipeline helpers ---
def _fetch_block_batch(heights, mode):
    """
    Fetch a contiguous batch of blocks:
    - getblockhash for heights
    - getblock (auto-verbose) for valid hashes
    Returns (valid_pairs, blocks) where:
      valid_pairs = [(height, block_hash), ...] only for hashes that resolved
      blocks      = list of block objects aligned to valid_pairs order
    """
    hashes = batch_getblockhashes(heights)
    valid_pairs = [(h, hh) for h, hh in zip(heights, hashes) if hh]
    hash_list = [hh for _, hh in valid_pairs]
    blocks = batch_getblocks_verbose_auto(hash_list, mode) if hash_list else []
    return valid_pairs, blocks


# FIFO multi-future prefetch pipeline helper
def _prefetch_pipeline_iter(start_h, end_h, mode, batch_size, executor, depth):
    """
    Yield (valid_pairs, blocks) for contiguous height batches while keeping up to `depth`
    batches in flight using `executor`. Order is preserved (FIFO).
    """
    cur = start_h
    in_flight = []

    # Prime the pipeline
    while cur <= end_h and len(in_flight) < max(1, depth):
        heights = list(range(cur, min(cur + batch_size - 1, end_h) + 1))
        fut = executor.submit(_fetch_block_batch, heights, mode)
        in_flight.append((heights[0], heights[-1], fut))
        cur = heights[-1] + 1

    # Drain while maintaining depth
    while in_flight:
        first_h, last_h, fut = in_flight.pop(0)
        try:
            valid_pairs, blocks = fut.result()
        except Exception as e:
            print(f"Prefetch failed for heights {first_h}..{last_h}: {e}")
            valid_pairs, blocks = [], []

        # Keep pipeline filled
        if cur <= end_h:
            heights = list(range(cur, min(cur + batch_size - 1, end_h) + 1))
            fut2 = executor.submit(_fetch_block_batch, heights, mode)
            in_flight.append((heights[0], heights[-1], fut2))
            cur = heights[-1] + 1

        yield valid_pairs, blocks


def batch_getrawtransactions(txids, verbose=True):
    if verbose:
        calls = [("getrawtransaction", [txid, 1]) for txid in txids]
    else:
        calls = [("getrawtransaction", [txid]) for txid in txids]
    results = rpc_batch(calls)
    out = []
    for item in results:
        if not item or item.get("error"):
            out.append(None)
        else:
            out.append(item.get("result"))
    return out


# Helper: fetch raw transactions in chunks to avoid overloading node
def batch_getrawtransactions_chunked(txids, verbose=True, chunk=TX_BATCH_CHUNK):
    """
    Fetch decoded transactions in chunks to avoid overloading the node.
    Returns a list of results in the same order as txids (None for failures).
    """
    results = []
    for i in range(0, len(txids), max(1, chunk)):
        part = txids[i:i+chunk]
        part_res = batch_getrawtransactions(part, verbose=verbose)
        results.extend(part_res)
    return results


def getblockcount():
    _response = rpc_request('getblockcount', [])
    if _response.get('error') is not None:
        print(f"Send transaction failed: {_response['error']}")
        return None
    return _response.get('result')


def get_block_hash(_block_nr):
    _response = rpc_request('getblockhash', [_block_nr])
    if _response.get('error') is not None:
        print(f"Send transaction failed: {_response['error']}")
        return None
    return _response.get('result')


def get_peers():
    _response = rpc_request('getpeerinfo', [])
    if _response.get('error') is not None:
        print(f"Send transaction failed: {_response['error']}")
        return None
    return _response.get('result')


def get_block_info(_block_hash):
    _response = rpc_request('getblock', [_block_hash])
    if _response.get('error') is not None:
        print(f"Send transaction failed: {_response['error']}")
        return None
    return _response.get('result')


def getrawtransaction(_tx_hash, verbose=True):
    """
    If verbose=True, returns the decoded transaction object directly (1-call).
    Otherwise returns the raw hex string.
    """
    params = [_tx_hash, 1] if verbose else [_tx_hash]
    _response = rpc_request('getrawtransaction', params)
    if _response.get('error') is not None:
        print(f"getrawtransaction failed: {_response['error']}")
        return None
    return _response.get('result')


def decoderawtransaction(_hex):
    _response = rpc_request('decoderawtransaction', [_hex])
    if _response.get('error') is not None:
        print(f"Send transaction failed: {_response['error']}")
        return None
    return _response.get('result')


def reinitialize_tables():
    conn = sqlite3.connect(DATABASE, timeout=10)
    c = conn.cursor()

    # List of tables to be deleted and newly created
    tables_to_drop = ['blocks', 'transactions', 'vin', 'vout', 'addresses']

    # Delete existing tables, if available
    for table in tables_to_drop:
        c.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"Table {table} dropped.")

    # Re-create table
    c.execute('''
    CREATE TABLE IF NOT EXISTS blocks (
        block_hash TEXT PRIMARY KEY,
        confirmations INTEGER NOT NULL,
        block_size INTEGER NOT NULL,
        block_height INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        nonce INTEGER NOT NULL,
        difficulty REAL NOT NULL,
        prev_hash TEXT,
        flags TEXT,
        effective_burn_coins REAL DEFAULT 0,
        mint REAL DEFAULT 0,
        burnt REAL DEFAULT 0
    )
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        txid TEXT PRIMARY KEY,
        block_hash TEXT NOT NULL,
        amount REAL NOT NULL,
        timestamp INTEGER NOT NULL,
        is_coinbase BOOLEAN NOT NULL,
        FOREIGN KEY (block_hash) REFERENCES blocks(block_hash)
    )
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS vin (
        txid TEXT NOT NULL,
        vout_txid TEXT NOT NULL,
        vout_index INTEGER NOT NULL,
        FOREIGN KEY (txid) REFERENCES transactions(txid),
        FOREIGN KEY (vout_txid, vout_index) REFERENCES vout(txid, ind)
    )
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS vout (
        txid TEXT NOT NULL,
        ind INTEGER NOT NULL,
        amount REAL NOT NULL,
        address TEXT NOT NULL,
        spent BOOLEAN NOT NULL,
        block_hash TEXT NOT NULL,
        created_block_height INTEGER NOT NULL,
        FOREIGN KEY (txid) REFERENCES transactions(txid),
        FOREIGN KEY (block_hash) REFERENCES blocks(block_hash)
    )
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS addresses (
        address TEXT PRIMARY KEY,
        total_received REAL NOT NULL,
        total_sent REAL NOT NULL,
        balance REAL NOT NULL,
        last_updated INTEGER NOT NULL
    )
    ''')

    conn.commit()
    conn.close()
    print("All tables reinitialized.")


def create_indices(conn):
    c = conn.cursor()
    # --- Existing helpful indexes ---
    c.execute('CREATE INDEX IF NOT EXISTS idx_blocks_height   ON blocks(block_height)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tx_block        ON transactions(block_hash)')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_vout_txn ON vout(txid, ind)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vout_addr       ON vout(address)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vin_outref      ON vin(vout_txid, vout_index)')

    # --- Additions for runtime performance (richlist, address, and purge speed) ---
    # Composite for address lookups that also filter by spent
    c.execute('CREATE INDEX IF NOT EXISTS idx_vout_addr_spent ON vout(address, spent)')
    # Partial index for unspent-per-address (fast balance/richlist during reindex, if SQLite supports partial indexes)
    c.execute('CREATE INDEX IF NOT EXISTS idx_vout_addr_unspent ON vout(address) WHERE spent = 0')
    # Speed up deletions/purges and joins by block
    c.execute('CREATE INDEX IF NOT EXISTS idx_vout_blockhash   ON vout(block_hash)')
    # Speed up purge step which deletes vin rows by their own txid
    c.execute('CREATE INDEX IF NOT EXISTS idx_vin_txid         ON vin(txid)')
    # Help ORDER BY balance DESC LIMIT N on /richlist
    c.execute('CREATE INDEX IF NOT EXISTS idx_addresses_balance ON addresses(balance)')

    c.close()


def drop_indices(conn):
    # Safe even if they don't exist yet
    c = conn.cursor()
    for idx in [
        'idx_blocks_height',
        'idx_tx_block',
        'idx_vout_txn',
        'idx_vout_addr',
        'idx_vin_outref',
        'idx_vout_addr_spent',
        'idx_vout_addr_unspent',
        'idx_vout_blockhash',
        'idx_vin_txid',
        'idx_addresses_balance',
    ]:
        c.execute(f'DROP INDEX IF EXISTS {idx}')
    c.close()

# --- Ensure schema exists for incremental update ---
def ensure_tables_exist(conn):
    """
    Create required explorer tables if they are missing (non-destructive).
    This allows update_with_latest_block() to run on a fresh DB without reindex.py.
    """
    c = conn.cursor()
    try:
        # blocks
        c.execute('''
            CREATE TABLE IF NOT EXISTS blocks (
                block_hash TEXT PRIMARY KEY,
                confirmations INTEGER NOT NULL,
                block_size INTEGER NOT NULL,
                block_height INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                nonce INTEGER NOT NULL,
                difficulty REAL NOT NULL,
                prev_hash TEXT,
                flags TEXT,
                effective_burn_coins REAL DEFAULT 0,
                mint REAL DEFAULT 0,
                burnt REAL DEFAULT 0
            )
        ''')
        # transactions
        c.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                txid TEXT PRIMARY KEY,
                block_hash TEXT NOT NULL,
                amount REAL NOT NULL,
                timestamp INTEGER NOT NULL,
                is_coinbase BOOLEAN NOT NULL,
                FOREIGN KEY (block_hash) REFERENCES blocks(block_hash)
            )
        ''')
        # vin
        c.execute('''
            CREATE TABLE IF NOT EXISTS vin (
                txid TEXT NOT NULL,
                vout_txid TEXT NOT NULL,
                vout_index INTEGER NOT NULL,
                FOREIGN KEY (txid) REFERENCES transactions(txid),
                FOREIGN KEY (vout_txid, vout_index) REFERENCES vout(txid, ind)
            )
        ''')
        # vout
        c.execute('''
            CREATE TABLE IF NOT EXISTS vout (
                txid TEXT NOT NULL,
                ind INTEGER NOT NULL,
                amount REAL NOT NULL,
                address TEXT NOT NULL,
                spent BOOLEAN NOT NULL,
                block_hash TEXT NOT NULL,
                created_block_height INTEGER NOT NULL,
                FOREIGN KEY (txid) REFERENCES transactions(txid),
                FOREIGN KEY (block_hash) REFERENCES blocks(block_hash)
            )
        ''')
        # addresses
        c.execute('''
            CREATE TABLE IF NOT EXISTS addresses (
                address TEXT PRIMARY KEY,
                total_received REAL NOT NULL,
                total_sent REAL NOT NULL,
                balance REAL NOT NULL,
                last_updated INTEGER NOT NULL
            )
        ''')
        # indices (safe if exist)
        create_indices(conn)
        conn.commit()
    finally:
        c.close()


def _purge_from_height(conn, start_height: int):
    """
    Delete all rows at or after a given block height so we can safely rebuild
    from that height without unique/duplicate conflicts. Also rebuilds 'spent'
    flags from remaining vin rows to keep the DB consistent for earlier blocks.
    """
    if start_height is None or start_height < 1:
        return
    c = conn.cursor()
    try:
        # Make sure schema exists
        ensure_tables_exist(conn)

        # 1) vin deren EIGENE tx in zu löschenden Blöcken liegt
        c.execute('''
            DELETE FROM vin
            WHERE txid IN (
                SELECT t.txid
                FROM transactions t
                JOIN blocks b ON t.block_hash = b.block_hash
                WHERE b.block_height >= ?
            )
        ''', (start_height,))
        # 2) vout, die in den zu löschenden Blöcken erstellt wurden
        c.execute('''
            DELETE FROM vout
            WHERE block_hash IN (
                SELECT block_hash FROM blocks WHERE block_height >= ?
            )
        ''', (start_height,))
        # 3) transactions der zu löschenden Blöcke
        c.execute('''
            DELETE FROM transactions
            WHERE block_hash IN (
                SELECT block_hash FROM blocks WHERE block_height >= ?
            )
        ''', (start_height,))
        # 4) schließlich die Blöcke selbst
        c.execute('DELETE FROM blocks WHERE block_height >= ?', (start_height,))

        # Spent-Flags aus verbliebenen vin-Daten neu aufbauen
        c.execute('UPDATE vout SET spent = 0')
        c.execute('''
            UPDATE vout
            SET spent = 1
            WHERE EXISTS (
                SELECT 1 FROM vin
                WHERE vin.vout_txid = vout.txid
                  AND vin.vout_index = vout.ind
            )
        ''')

        conn.commit()
    except Exception as e:
        print(f"Error purging from height {start_height}: {e}")
        conn.rollback()
    finally:
        c.close()


def replace_block_in_db(_block_info, current_height, conn):
    _block_hash = _block_info['hash']
    _block_size = int(_block_info.get('size', 0))
    _block_height = int(_block_info.get('height', 0))
    _timestamp = _normalize_block_time(_block_info.get('time'))
    _nonce = int(_block_info.get('nonce', 0))
    _difficulty = float(_block_info.get('difficulty', 0))
    _prev_hash = _block_info.get('previousblockhash')  # may be None on genesis
    _flags = _block_info.get('flags')
    _effective_burn_coins = float((_block_info.get('nEffectiveBurnCoins') or 0))
    _mint = float((_block_info.get('mint') or 0))
    _burnt = float((_block_info.get('burnt') or 0))
    _confirmations = current_height - _block_height + 1 if (current_height and _block_height) else 0

    c = conn.cursor()
    c.execute('REPLACE INTO blocks (block_hash, confirmations, block_size, block_height, timestamp, nonce, '
              'difficulty, prev_hash, flags, effective_burn_coins, mint, burnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
              (_block_hash, _confirmations, _block_size, _block_height, _timestamp, _nonce, _difficulty,
               _prev_hash, _flags, _effective_burn_coins, _mint, _burnt))
    c.close()


def update_peers():
    # Connect to database
    getpeerinfo = get_peers()
    conn = sqlite3.connect(PEERS, timeout=5)
    c = conn.cursor()

    # Create Table in case it doesn't exist.
    c.execute('''
        CREATE TABLE IF NOT EXISTS peers (
            peer_ip TEXT NOT NULL UNIQUE)
    ''')

    # Delete old peer data to keep the table up-to-date
    c.execute('DELETE FROM peers')

    # Add new peer data
    peer_ips = [(peer['addr'],) for peer in getpeerinfo]
    print(peer_ips)
    c.executemany('INSERT INTO peers (peer_ip) VALUES (?)', peer_ips)

    # Save database changes and close connection
    conn.commit()
    conn.close()


def update_address_in_db(address, received, sent, current_block_height, conn):
    if not address:
        return  # don't create empty-address rows
    c = conn.cursor()
    try:
        # Fetch current totals (if any)
        c.execute('SELECT total_received, total_sent FROM addresses WHERE address = ?', (address,))
        result = c.fetchone()
        if result:
            total_received, total_sent = map(Decimal, result)
            new_total_received = total_received + Decimal(received)
            new_total_sent = total_sent + Decimal(sent)
        else:
            new_total_received = Decimal(received)
            new_total_sent = Decimal(sent)

        # Unspent balance for the address
        c.execute('SELECT SUM(amount) FROM vout WHERE address = ? AND spent = 0', (address,))
        unspent_total = c.fetchone()[0] or 0
        unspent_total_str = str(Decimal(unspent_total))

        # Upsert address row (no commit here; caller manages transactions)
        c.execute('''
            INSERT INTO addresses (address, total_received, total_sent, balance, last_updated)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                total_received = excluded.total_received,
                total_sent = excluded.total_sent,
                balance = excluded.balance,
                last_updated = excluded.last_updated
        ''', (address, str(new_total_received), str(new_total_sent), unspent_total_str, current_block_height))
    except Exception as e:
        print(f"Error updating address in DB: {e}")
    finally:
        c.close()


def update_all_addresses(current_block_height):
    with sqlite3.connect(DATABASE, timeout=5) as conn:
        c = conn.cursor()
        try:
            # Call all addresses
            c.execute('SELECT DISTINCT address FROM vout WHERE spent = 0')
            addresses = c.fetchall()

            # Update every single address
            for (address,) in addresses:
                # Retrieve all unused transaction outputs (UTXOs) of the address
                c.execute('''
                    SELECT amount, created_block_height FROM vout
                    WHERE address = ? AND spent = 0
                ''', (address,))
                vouts = c.fetchall()

                new_balance = Decimal(0)
                for amount, created_block_height in vouts:
                    amount = Decimal(amount)
                    new_balance += amount

                # Retrieve the current last update
                c.execute('SELECT last_updated FROM addresses WHERE address = ?', (address,))
                last_updated = c.fetchone()[0]

                # Update the address in the database
                c.execute(
                    'UPDATE addresses SET balance = ?, last_updated = ? WHERE address = ?',
                    (str(new_balance), current_block_height, address))
                print(f"Updated {address} from {last_updated} to: {new_balance}")

            conn.commit()
        except Exception as e:
            print(f"Error updating all addresses: {e}")
        finally:
            c.close()


def replace_transaction_in_db(tx, block_height, block_hash, time, conn, do_address_updates=True):
    c = conn.cursor()
    try:
        if 'txid' not in tx:
            print("Missing data in the transaction: ", tx)
            return

        total_amount = sum(v.get('value', 0) for v in tx.get('vout', []))
        is_coinbase = bool(tx.get('vin') and 'coinbase' in tx['vin'][0])

        c.execute('''
            INSERT INTO transactions (txid, block_hash, amount, timestamp, is_coinbase)
            VALUES (?, ?, ?, ?, ?)
        ''', (tx['txid'], block_hash, total_amount, time, is_coinbase))

        # vout: credit receivers
        for vout in tx.get('vout', []):
            spk = vout.get('scriptPubKey', {})
            address = ''
            if isinstance(spk.get('addresses'), list) and spk.get('addresses'):
                address = spk['addresses'][0]
            elif 'address' in spk:
                address = spk['address']
            amount = vout.get('value', 0)
            if not address:
                continue
            c.execute('''
                INSERT INTO vout (txid, ind, amount, address, spent, block_hash, created_block_height)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (tx['txid'], vout.get('n', 0), amount, address, False, block_hash, block_height))
            if do_address_updates:
                update_address_in_db(address, amount, 0, block_height, conn)

        # vin: spend & debit
        for vin in tx.get('vin', []):
            if 'coinbase' in vin:
                continue
            vout_txid = vin.get('txid')
            vout_index = vin.get('vout')
            if vout_txid is None or vout_index is None:
                continue
            c.execute('INSERT INTO vin (txid, vout_txid, vout_index) VALUES (?, ?, ?)',
                      (tx['txid'], vout_txid, vout_index))
            c.execute('UPDATE vout SET spent = 1 WHERE txid = ? AND ind = ?', (vout_txid, vout_index))
            c.execute('SELECT address, amount FROM vout WHERE txid = ? AND ind = ?', (vout_txid, vout_index))
            vout_details = c.fetchone()
            if vout_details:
                if do_address_updates:
                    update_address_in_db(vout_details[0], 0, vout_details[1], block_height, conn)
    except Exception as e:
        print(f"Failed to insert transaction data into database: {e}")
    finally:
        c.close()


def rebuild_addresses(conn, current_block_height):
    """
    Recompute addresses in bulk from vout only (fast path for reindex):
      - total_received: SUM(vout.amount) per address
      - balance: SUM(vout.amount WHERE spent=0) per address
      - total_sent: total_received - balance
    This avoids per-transaction address updates during reindex.
    """
    c = conn.cursor()
    try:
        # Clear addresses and rebuild from aggregates
        c.execute('DELETE FROM addresses')
        c.execute('''
            INSERT INTO addresses (address, total_received, total_sent, balance, last_updated)
            SELECT r.address,
                   r.total_received,
                   r.total_received - IFNULL(u.balance, 0) AS total_sent,
                   IFNULL(u.balance, 0) AS balance,
                   ?
            FROM (
                SELECT address, SUM(amount) AS total_received
                FROM vout
                GROUP BY address
            ) AS r
            LEFT JOIN (
                SELECT address, SUM(amount) AS balance
                FROM vout
                WHERE spent = 0
                GROUP BY address
            ) AS u ON r.address = u.address
        ''', (current_block_height,))
        conn.commit()
    except Exception as e:
        print(f"Error rebuilding addresses: {e}")
        conn.rollback()
    finally:
        c.close()


def reindex_db(start_height=None):
    """
    Full reindex when start_height is None (drop & recreate tables).
    Partial reindex when start_height >= 1: purge data from that height and rebuild to tip.
    """
    if start_height is None:
        # Original "full" reindex behaviour
        reinitialize_tables()
        conn = sqlite3.connect(DATABASE, timeout=10)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA cache_size=-20000')
            conn.execute('PRAGMA foreign_keys=OFF')
            conn.execute('PRAGMA busy_timeout=5000')

            drop_indices(conn)

            c = conn.cursor()
            c.execute('SELECT MAX(block_height) FROM blocks')
            result = c.fetchone()
            last_saved_block_height = result[0] if result and result[0] is not None else 0
            print(f"Last saved block height: {last_saved_block_height}")

            current_block_height = int(getblockcount() or 0)
            print(f"Current block height: {current_block_height}")

            if current_block_height > last_saved_block_height:
                print(f"Updating database with new blocks from {last_saved_block_height + 1} to {current_block_height}.")
                start_h = last_saved_block_height + 1
                end_h = current_block_height
                idx = 0
                h = start_h

                mode = resolve_verbose_mode()
                print(f"[index] getblock verbose mode: {mode}")

                if not conn.in_transaction:
                    conn.execute('BEGIN IMMEDIATE')

                prefetch_pool = ThreadPoolExecutor(max_workers=PREFETCH_WORKERS)
                try:
                    for valid_pairs, blocks in _prefetch_pipeline_iter(h, end_h, mode, BATCH_SIZE, prefetch_pool, PREFETCH_DEPTH):
                        if not valid_pairs:
                            continue

                        per_block_entries = []
                        need_separate_tx_fetch = (mode == 'ids_only')
                        all_txids = []

                        for bi in blocks:
                            if not bi:
                                per_block_entries.append([])
                                continue
                            txs = bi.get('tx', [])
                            if isinstance(txs, list) and txs and isinstance(txs[0], dict):
                                per_block_entries.append(txs)
                            else:
                                per_block_entries.append(txs)
                                if need_separate_tx_fetch:
                                    all_txids.extend(txs)

                        decoded_map = {}
                        if need_separate_tx_fetch and all_txids:
                            tx_results = batch_getrawtransactions_chunked(all_txids, verbose=True, chunk=TX_BATCH_CHUNK)
                            for txid, t in zip(all_txids, tx_results):
                                if t:
                                    decoded_map[txid] = t

                        block_iter = iter(blocks)
                        for (height, hh), txs in zip(valid_pairs, per_block_entries):
                            bi = next(block_iter, None)
                            if not bi:
                                print(f"Failed to get block for height {height}.")
                                continue

                            replace_block_in_db(bi, current_block_height, conn)
                            time_epoch = _normalize_block_time(bi.get('time'))

                            if txs:
                                if isinstance(txs[0], dict):
                                    for t in txs:
                                        replace_transaction_in_db(t, height, hh, time_epoch, conn, do_address_updates=True)
                                else:
                                    for txid in txs:
                                        t = decoded_map.get(txid)
                                        if t is None:
                                            t = getrawtransaction(txid, verbose=True)
                                            if not t:
                                                continue
                                        replace_transaction_in_db(t, height, hh, time_epoch, conn, do_address_updates=True)

                            idx += 1
                            if idx % COMMIT_INTERVAL == 0:
                                conn.commit()
                                conn.execute('BEGIN IMMEDIATE')
                                print(f"… up to block {height}")

                        h = valid_pairs[-1][0] + 1
                finally:
                    prefetch_pool.shutdown(wait=True)

                conn.commit()
                create_indices(conn)
                conn.commit()
                rebuild_addresses(conn, current_block_height)
            else:
                print("No new blocks to add.")
        except Exception as e:
            print(f"Failed to update with latest blocks: {e}")
            conn.rollback()
        finally:
            conn.close()
        return

    # ---- Partial reindex path (start_height provided) ----
    start_h = max(1, int(start_height))
    conn = sqlite3.connect(DATABASE, timeout=10)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA cache_size=-20000')
        conn.execute('PRAGMA foreign_keys=OFF')
        conn.execute('PRAGMA busy_timeout=5000')

        ensure_tables_exist(conn)

        current_block_height = int(getblockcount() or 0)
        print(f"[partial reindex] Purging from height {start_h}, tip is {current_block_height}")

        drop_indices(conn)
        _purge_from_height(conn, start_h)

        mode = resolve_verbose_mode()
        print(f"[partial] getblock verbose mode: {mode}")

        end_h = current_block_height
        if start_h > end_h:
            print("Start height is above current tip; nothing to do.")
            create_indices(conn)
            conn.commit()
            return

        if not conn.in_transaction:
            conn.execute('BEGIN IMMEDIATE')

        prefetch_pool = ThreadPoolExecutor(max_workers=PREFETCH_WORKERS)
        try:
            idx = 0
            h = start_h
            for valid_pairs, blocks in _prefetch_pipeline_iter(h, end_h, mode, BATCH_SIZE, prefetch_pool, PREFETCH_DEPTH):
                if not valid_pairs:
                    continue

                per_block_entries = []
                need_separate_tx_fetch = (mode == 'ids_only')
                all_txids = []

                for bi in blocks:
                    if not bi:
                        per_block_entries.append([])
                        continue
                    txs = bi.get('tx', [])
                    if isinstance(txs, list) and txs and isinstance(txs[0], dict):
                        per_block_entries.append(txs)
                    else:
                        per_block_entries.append(txs)
                        if need_separate_tx_fetch:
                            all_txids.extend(txs)

                decoded_map = {}
                if need_separate_tx_fetch and all_txids:
                    tx_results = batch_getrawtransactions_chunked(all_txids, verbose=True, chunk=TX_BATCH_CHUNK)
                    for txid, t in zip(all_txids, tx_results):
                        if t:
                            decoded_map[txid] = t

                block_iter = iter(blocks)
                for (height, hh), txs in zip(valid_pairs, per_block_entries):
                    bi = next(block_iter, None)
                    if not bi:
                        print(f"Failed to get block for height {height}.")
                        continue

                    replace_block_in_db(bi, current_block_height, conn)
                    time_epoch = _normalize_block_time(bi.get('time'))

                    if txs:
                        if isinstance(txs[0], dict):
                            for t in txs:
                                replace_transaction_in_db(t, height, hh, time_epoch, conn, do_address_updates=True)
                        else:
                            for txid in txs:
                                t = decoded_map.get(txid)
                                if t is None:
                                    t = getrawtransaction(txid, verbose=True)
                                    if not t:
                                        continue
                                replace_transaction_in_db(t, height, hh, time_epoch, conn, do_address_updates=True)

                    idx += 1
                    if idx % COMMIT_INTERVAL == 0:
                        conn.commit()
                        conn.execute('BEGIN IMMEDIATE')
                        print(f"… up to block {height}")

                h = valid_pairs[-1][0] + 1
        finally:
            prefetch_pool.shutdown(wait=True)

        conn.commit()
        create_indices(conn)
        conn.commit()
        rebuild_addresses(conn, current_block_height)
    except Exception as e:
        print(f"Failed partial reindex from {start_h}: {e}")
        conn.rollback()
    finally:
        conn.close()


def reindex_db_continue(rewind: int = CONTINUE_REWIND):
    """
    Resume indexing, but first rewind N blocks (default CONTINUE_REWIND=10) to
    avoid inconsistencies if the last run was interrupted. This purges data
    from the rewind start height so there are no duplicate key conflicts.
    """
    conn = sqlite3.connect(DATABASE, timeout=10)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA cache_size=-20000')
        conn.execute('PRAGMA foreign_keys=OFF')
        conn.execute('PRAGMA busy_timeout=5000')

        ensure_tables_exist(conn)

        c = conn.cursor()
        c.execute('SELECT MAX(block_height) FROM blocks')
        result = c.fetchone()
        last_saved_block_height = result[0] if result and result[0] is not None else 0
        print(f"Last saved block height: {last_saved_block_height}")

        current_block_height = int(getblockcount() or 0)
        print(f"Current block height: {current_block_height}")

        if current_block_height > last_saved_block_height:
            start_h = max(1, last_saved_block_height - int(rewind))
            end_h = current_block_height
            print(f"Continuing with rewind={rewind}: rebuilding from {start_h} to {end_h}.")

            _purge_from_height(conn, start_h)

            idx = 0
            h = start_h

            mode = resolve_verbose_mode()
            print(f"[continue] getblock verbose mode: {mode}")

            if not conn.in_transaction:
                conn.execute('BEGIN IMMEDIATE')

            prefetch_pool = ThreadPoolExecutor(max_workers=PREFETCH_WORKERS)
            try:
                for valid_pairs, blocks in _prefetch_pipeline_iter(h, end_h, mode, BATCH_SIZE, prefetch_pool, PREFETCH_DEPTH):
                    if not valid_pairs:
                        continue

                    per_block_entries = []
                    need_separate_tx_fetch = (mode == 'ids_only')
                    all_txids = []

                    for bi in blocks:
                        if not bi:
                            per_block_entries.append([])
                            continue
                        txs = bi.get('tx', [])
                        if isinstance(txs, list) and txs and isinstance(txs[0], dict):
                            per_block_entries.append(txs)
                        else:
                            per_block_entries.append(txs)
                            if need_separate_tx_fetch:
                                all_txids.extend(txs)

                    decoded_map = {}
                    if need_separate_tx_fetch and all_txids:
                        tx_results = batch_getrawtransactions_chunked(all_txids, verbose=True, chunk=TX_BATCH_CHUNK)
                        for txid, t in zip(all_txids, tx_results):
                            if t:
                                decoded_map[txid] = t

                    block_iter = iter(blocks)
                    for (height, hh), txs in zip(valid_pairs, per_block_entries):
                        bi = next(block_iter, None)
                        if not bi:
                            print(f"Failed to get block for height {height}.")
                            continue

                        replace_block_in_db(bi, current_block_height, conn)
                        time_epoch = _normalize_block_time(bi.get('time'))

                        if txs:
                            if isinstance(txs[0], dict):
                                for t in txs:
                                    replace_transaction_in_db(t, height, hh, time_epoch, conn, do_address_updates=True)
                            else:
                                for txid in txs:
                                    t = decoded_map.get(txid)
                                    if t is None:
                                        t = getrawtransaction(txid, verbose=True)
                                        if not t:
                                            continue
                                    replace_transaction_in_db(t, height, hh, time_epoch, conn, do_address_updates=True)

                        idx += 1
                        if idx % COMMIT_INTERVAL == 0:
                            conn.commit()
                            conn.execute('BEGIN IMMEDIATE')
                            print(f"… up to block {height}")

                    h = valid_pairs[-1][0] + 1
            finally:
                prefetch_pool.shutdown(wait=True)

            conn.commit()
            rebuild_addresses(conn, current_block_height)
        else:
            print("No new blocks to add.")
    except Exception as e:
        print(f"Failed to continue indexing: {e}")
        conn.rollback()
    finally:
        conn.close()


def update_with_latest_block():
    with sqlite3.connect(DATABASE, timeout=5) as conn:
        # Ensure schema exists so we can run without a prior reindex
        ensure_tables_exist(conn)
        c = conn.cursor()
        try:
            c.execute('SELECT MAX(block_height) FROM blocks')
            result = c.fetchone()
            last_saved_block_height = result[0] if result and result[0] is not None else 0

            current_block_height = int(getblockcount() or 0)

            if current_block_height > last_saved_block_height:
                print(f"Updating database with new blocks from {last_saved_block_height + 1} to {current_block_height}.")
                block_height = last_saved_block_height + 1
                while block_height <= current_block_height:
                    block_hash = get_block_hash(block_height)
                    if not block_hash:
                        print(f"Failed to get block hash for height {block_height}.")
                        # Still commit periodically to persist earlier progress
                        if INCR_COMMIT_EVERY <= 1 or (block_height % INCR_COMMIT_EVERY == 0):
                            try:
                                conn.commit()
                            except Exception as e:
                                print(f"Commit failed at height {block_height} (no block): {e}")
                        block_height += 1
                        continue

                    print(f"Adding block {block_height} to database: {block_hash}")
                    block_info = get_block_info(block_hash)
                    if block_info:
                        if block_height > 0 and block_info.get('previousblockhash'):
                            c.execute('SELECT block_hash FROM blocks WHERE block_height = ?', (block_height - 1,))
                            previous_hash = c.fetchone()
                            if previous_hash and previous_hash[0] != block_info['previousblockhash']:
                                print(f"Detected a block chain discontinuity at {block_height}, attempting to fix.")
                                block_height = max(0, block_height - 3)
                                continue

                        replace_block_in_db(block_info, current_block_height, conn)
                        time_epoch = _normalize_block_time(block_info.get('time'))
                        for tx_id in block_info.get('tx', []):
                            decoded_tx = getrawtransaction(tx_id, verbose=True)
                            if not decoded_tx:
                                continue
                            replace_transaction_in_db(decoded_tx, block_height, block_hash, time_epoch, conn)
                        # Commit progress periodically (or every block by default)
                        if INCR_COMMIT_EVERY <= 1 or (block_height % INCR_COMMIT_EVERY == 0):
                            try:
                                conn.commit()
                            except Exception as e:
                                print(f"Commit failed at height {block_height}: {e}")
                        block_height += 1
                    else:
                        print(f"Failed to get block info for block {block_hash}.")
                        # Still commit periodically to persist earlier progress
                        if INCR_COMMIT_EVERY <= 1 or (block_height % INCR_COMMIT_EVERY == 0):
                            try:
                                conn.commit()
                            except Exception as e:
                                print(f"Commit failed at height {block_height} (no block): {e}")
                        block_height += 1

                update_all_addresses(current_block_height)
                conn.commit()
            else:
                print("No new blocks to add.")
        except Exception as e:
            print(f"Failed to update with latest blocks: {e}")
            conn.rollback()


def get_address_balance(address):
    with sqlite3.connect(DATABASE, timeout=5) as conn:
        c = conn.cursor()
        c.execute('SELECT balance FROM addresses WHERE address = ?', (address,))
        row = c.fetchone()
        if row is None or row[0] is None:
            return None
        return {"balance": float(row[0])}


def calculate_total_supply():
    """
    Returns the chain total supply as:
        total_supply = SUM(mint) - burnt_at_tip
    where 'burnt' is a cumulative value stored on the latest block (chain tip).
    Falls back to SUM(addresses.balance) if block metadata is not yet populated.
    """
    with sqlite3.connect(DATABASE, timeout=5) as conn:
        c = conn.cursor()
        # 1) cumulative minted across all blocks
        c.execute('SELECT SUM(mint) FROM blocks')
        row = c.fetchone()
        minted_sum = Decimal(row[0] or 0)
        # 2) cumulative burnt from the latest block (tip)
        c.execute('SELECT burnt FROM blocks ORDER BY block_height DESC LIMIT 1')
        row2 = c.fetchone()
        burnt_cum = Decimal(row2[0] or 0) if row2 else Decimal(0)
        total = minted_sum - burnt_cum
        # Fallback if DB not yet indexed
        if minted_sum == 0 and burnt_cum == 0:
            c.execute('SELECT SUM(balance) FROM addresses')
            res = c.fetchone()
            return float(res[0] or 0)
        return float(total)


# Helper: get total burnt coins from block metadata
def get_total_burnt():
    """
    Returns the cumulative amount burned (value from the latest block).
    """
    with sqlite3.connect(DATABASE, timeout=5) as conn:
        c = conn.cursor()
        c.execute('SELECT burnt FROM blocks ORDER BY block_height DESC LIMIT 1')
        row = c.fetchone()
        return float(row[0] or 0) if row else 0.0


def test_get_address_info(address):
    with sqlite3.connect(DATABASE, timeout=5) as conn:
        c = conn.cursor()
        c.execute('SELECT address, '
                  'total_received, '
                  'total_sent, '
                  'balance, '
                  'last_updated FROM addresses WHERE address = ?', (address,))
        result = c.fetchone()
        if result:
            _address, total_received, total_sent, balance, last_updated = result
            return {'address': _address,
                    'total_received': total_received,
                    'total_sent': total_sent,
                    'balance': balance,
                    'last_updated': last_updated}
        else:
            return None



