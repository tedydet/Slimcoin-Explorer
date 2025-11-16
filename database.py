import sqlite3
import requests
import json
from dotenv import load_dotenv
import os
from decimal import Decimal
from datetime import datetime, timezone

# Reusable HTTP session for RPC calls
_session = requests.Session()
_session.headers.update({'content-type': 'application/json'})


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
BATCH_SIZE = int(os.getenv("INDEX_BATCH_SIZE", "50"))

DATABASE = 'blockchain.db'
PEERS = 'peers.db'


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
    conn = sqlite3.connect(DATABASE)
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
    c.execute('CREATE INDEX IF NOT EXISTS idx_blocks_height   ON blocks(block_height)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tx_block        ON transactions(block_hash)')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_vout_txn ON vout(txid, ind)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vout_addr       ON vout(address)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_vin_outref      ON vin(vout_txid, vout_index)')
    c.close()


def drop_indices(conn):
    # Safe even if they don't exist yet
    c = conn.cursor()
    for idx in [
        'idx_blocks_height',
        'idx_tx_block',
        'idx_vout_txn',
        'idx_vout_addr',
        'idx_vin_outref'
    ]:
        c.execute(f'DROP INDEX IF EXISTS {idx}')
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
    conn = sqlite3.connect(PEERS)
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
    with sqlite3.connect(DATABASE) as conn:
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


def replace_transaction_in_db(tx, block_height, block_hash, time, conn):
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
                update_address_in_db(vout_details[0], 0, vout_details[1], block_height, conn)
    except Exception as e:
        print(f"Failed to insert transaction data into database: {e}")
    finally:
        c.close()


def reindex_db():
    reinitialize_tables()
    conn = sqlite3.connect(DATABASE)
    try:
        # Fast bulk settings (safe for offline reindex)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=OFF')
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA cache_size=-20000')

        # Drop indices for fastest bulk insert; recreate at the end
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
            # BATCH_SIZE is now only the global value
            start_h = last_saved_block_height + 1
            end_h = current_block_height
            idx = 0
            h = start_h
            # Start first explicit transaction once
            if not conn.in_transaction:
                conn.execute('BEGIN')
            while h <= end_h:
                # Build height batch
                heights = list(range(h, min(h + BATCH_SIZE - 1, end_h) + 1))
                # 1) getblockhash in batch
                hashes = batch_getblockhashes(heights)

                # 2)keep valid pairs
                valid_pairs = [(height, hh) for height, hh in zip(heights, hashes) if hh]

                # 3) getblock in batch using exact order from valid pairs
                blocks = batch_getblocks([hh for _, hh in valid_pairs])
                if not blocks:
                    print(f"Batch getblock failed for heights {heights[0]}..{heights[-1]}")
                    h += BATCH_SIZE
                    continue
                block_iter = iter(blocks)

                # 4) Now run through heights again in the original order.
                for height, hh in zip(heights, hashes):
                    if not hh:
                        print(f"Failed to get block hash for height {height}.")
                        continue
                    bi = next(block_iter, None)
                    if not bi:
                        print(f"Failed to get block for height {height}.")
                        continue

                    replace_block_in_db(bi, current_block_height, conn)
                    time_epoch = _normalize_block_time(bi.get('time'))

                    txids = bi.get('tx', [])
                    if txids:
                        txs = batch_getrawtransactions(txids, verbose=True)
                        for t, txid in zip(txs, txids):
                            if not t:
                                t = getrawtransaction(txid, verbose=True)
                                if not t:
                                    continue
                            replace_transaction_in_db(t, height, hh, time_epoch, conn)

                    idx += 1
                    if idx % 100 == 0:
                        conn.commit()
                        conn.execute('BEGIN')
                        print(f"… up to block {height}")

                # advance to next batch
                h += BATCH_SIZE

            # Final commit before post-processing
            conn.commit()
            # Recreate indices after bulk load to speed up subsequent queries
            create_indices(conn)
            conn.commit()
            update_all_addresses(current_block_height)
        else:
            print("No new blocks to add.")
    except Exception as e:
        print(f"Failed to update with latest blocks: {e}")
        conn.rollback()
    finally:
        conn.close()


def update_with_latest_block():
    with sqlite3.connect(DATABASE) as conn:
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
                        block_height += 1
                    else:
                        print(f"Failed to get block info for block {block_hash}.")
                        block_height += 1

                update_all_addresses(current_block_height)
                conn.commit()
            else:
                print("No new blocks to add.")
        except Exception as e:
            print(f"Failed to update with latest blocks: {e}")
            conn.rollback()


def get_address_balance(address):
    with sqlite3.connect(DATABASE) as conn:
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
    with sqlite3.connect(DATABASE) as conn:
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
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('SELECT burnt FROM blocks ORDER BY block_height DESC LIMIT 1')
        row = c.fetchone()
        return float(row[0] or 0) if row else 0.0


def test_get_address_info(address):
    with sqlite3.connect(DATABASE) as conn:
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



