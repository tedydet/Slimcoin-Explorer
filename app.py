from flask import Flask, render_template, request, redirect, url_for, jsonify
import sqlite3
from datetime import datetime
import re
from database import get_total_burnt, calculate_total_supply

app = Flask(__name__)
DATABASE = 'blockchain.db'
PEERS = 'peers.db'


def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def get_blocks():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        SELECT 
            b.block_height,
            b.block_hash,
            b.timestamp,
            b.flags,
            b.effective_burn_coins,
            b.mint,
            b.burnt,
            IFNULL(SUM(t.amount), 0) as total_amount
        FROM blocks b
        LEFT JOIN transactions t ON b.block_hash = t.block_hash
        GROUP BY 
            b.block_height, b.block_hash, b.timestamp,
            b.flags, b.effective_burn_coins, b.mint, b.burnt
        ORDER BY b.block_height DESC
    """)
    blocks_raw = c.fetchall()
    blocks = [
        {
            'block_height': block['block_height'],
            'block_hash': block['block_hash'],
            'timestamp': datetime.fromtimestamp(block['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
            'total_amount': block['total_amount'],
            'flags': block['flags'],
            'effective_burn_coins': block['effective_burn_coins'],
            'mint': block['mint'],
            'burnt': block['burnt'],
        } for block in blocks_raw
    ]
    conn.close()
    return blocks


def get_block_detail(block_hash):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM blocks WHERE block_hash = ?", (block_hash,))
    block = c.fetchone()
    c.execute("SELECT txid, amount FROM transactions WHERE block_hash = ?", (block_hash,))
    transactions = [{'txid': tx['txid'], 'amount': tx['amount']} for tx in c.fetchall()]
    block_details = {
        'block_height': block['block_height'],
        'block_hash': block['block_hash'],
        'timestamp': datetime.fromtimestamp(block['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
        'confirmations': block['confirmations'],
        'difficulty': block['difficulty'],
        'nonce': block['nonce'],
        'prev_hash': block['prev_hash'],
        'flags': block['flags'],
        'effective_burn_coins': block['effective_burn_coins'],
        'mint': block['mint'],
        'burnt': block['burnt'],
        'transactions': transactions
    }
    conn.close()
    return block_details


@app.route('/')
def index():
    blocks = get_blocks()
    return render_template('index.html', blocks=blocks)


@app.route('/stats')
def stats_frame():
    return blockchain_stats()  # Calls the function that renders the template for the blockchain statistics


@app.route('/block/<block_hash>')
def block_detail(block_hash):
    block = get_block_detail(block_hash)  # Function that calls block and transaction details
    return render_template('block_detail.html', block=block)


@app.route('/blocks_frame')
@app.route('/blocks_frame/<int:page>')
def blocks_frame(page=1):
    per_page = 10  # Number of blocks per page
    offset = (page - 1) * per_page
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM blocks")
    total_blocks = c.fetchone()[0]
    total_pages = (total_blocks + per_page - 1) // per_page  # Calculates total pages

    c.execute("""
        SELECT 
            b.block_height,
            b.block_hash,
            b.timestamp,
            b.flags,
            b.effective_burn_coins,
            b.mint,
            b.burnt,
            IFNULL(SUM(t.amount), 0) as total_amount
        FROM blocks b
        LEFT JOIN transactions t ON b.block_hash = t.block_hash
        GROUP BY 
            b.block_height, b.block_hash, b.timestamp,
            b.flags, b.effective_burn_coins, b.mint, b.burnt
        ORDER BY b.block_height DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset))
    blocks_raw = c.fetchall()
    blocks = [
        {
            'block_height': block['block_height'],
            'block_hash': block['block_hash'],
            'timestamp': datetime.fromtimestamp(block['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
            'total_amount': block['total_amount'],
            'flags': block['flags'],
            'effective_burn_coins': block['effective_burn_coins'],
            'mint': block['mint'],
            'burnt': block['burnt'],
        } for block in blocks_raw
    ]
    conn.close()

    # Calculates number of pages to be shown
    start_page = max(1, page - 2)
    end_page = min(total_pages, page + 2)
    pages = list(range(start_page, end_page + 1))

    return render_template('blocks_frame.html', blocks=blocks, page=page, total_pages=total_pages, pages=pages)


@app.route('/transaction/<txid>')
def transaction_detail(txid):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        SELECT tx.txid, tx.amount, tx.block_hash, b.timestamp, b.confirmations, tx.is_coinbase
        FROM transactions tx
        JOIN blocks b ON tx.block_hash = b.block_hash
        WHERE tx.txid = ?
    """, (txid,))
    transaction_row = c.fetchone()

    # Convert the Row object into a Dictionary
    transaction = dict(transaction_row)
    transaction['timestamp'] = datetime.fromtimestamp(transaction['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

    # Determine recipient addresses and amounts
    c.execute("""
        SELECT vout.address as recipient, SUM(vout.amount) as amount
        FROM vout
        WHERE vout.txid = ?
        GROUP BY vout.address
    """, (txid,))
    outputs = [{'recipient': row['recipient'], 'amount': row['amount']} for row in c.fetchall()]

    # Determine the sender addresses and amounts
    c.execute("""
        SELECT vout.address as sender, SUM(vout.amount) as amount
        FROM vin
        JOIN vout ON vin.vout_txid = vout.txid AND vin.vout_index = vout.ind
        WHERE vin.txid = ?
        GROUP BY vout.address
    """, (txid,))
    inputs = [{'sender': row['sender'], 'amount': row['amount']} for row in c.fetchall()]

    conn.close()

    mining_status = "Yes" if transaction['is_coinbase'] else "No"

    # Fetch reward consensus flag and effective burn coins if this is a block reward
    reward_flag = None
    effective_burn_coins = None
    try:
        if transaction.get('is_coinbase') and transaction.get('block_hash'):
            with get_db_connection() as conn2:
                c2 = conn2.cursor()
                c2.execute("SELECT flags, effective_burn_coins FROM blocks WHERE block_hash = ?", (transaction['block_hash'],))
                row = c2.fetchone()
                if row:
                    reward_flag = row['flags']
                    effective_burn_coins = row['effective_burn_coins']
    except Exception as e:
        print(f"Warn: could not fetch reward flag for {txid}: {e}")

    return render_template('transaction_detail.html',
                           transaction=transaction,
                           outputs=outputs,
                           inputs=inputs,
                           mining_status=mining_status,
                           reward_flag=reward_flag,
                           effective_burn_coins = float(round(((effective_burn_coins or 0) / 1_000_000), 5)))


@app.route('/address/<address>')
@app.route('/address/<address>/<int:page>')
def address_detail(address, page=1):
    per_page = 50
    offset = (page - 1) * per_page
    conn = get_db_connection()
    c = conn.cursor()

    # Query the address totals
    c.execute("""
            SELECT SUM(amount) AS total_received
            FROM vout
            WHERE address = ?
        """, (address,))
    total_received = c.fetchone()[0]

    c.execute("""
            SELECT SUM(v.amount) AS total_sent
            FROM vin i
            JOIN vout v ON i.vout_txid = v.txid AND i.vout_index = v.ind
            WHERE v.address = ?
        """, (address,))
    total_sent = c.fetchone()[0]

    c.execute("""
            SELECT balance
            FROM addresses
            WHERE address = ?
        """, (address,))
    row = c.fetchone()
    balance = row['balance'] if row else 0

    # Calculation of the total number of transactions and pages
    c.execute("""
        SELECT COUNT(*) FROM (
            SELECT t.txid
            FROM vout v
            JOIN transactions t ON v.txid = t.txid
            WHERE v.address = ?
            GROUP BY t.txid
            UNION ALL
            SELECT i.txid
            FROM vin i
            JOIN vout v ON i.vout_txid = v.txid AND i.vout_index = v.ind
            JOIN transactions t ON i.txid = t.txid
            WHERE v.address = ? AND t.is_coinbase = 0
            GROUP BY i.txid
        ) AS combined
    """, (address, address))
    total_transactions = c.fetchone()[0]
    total_pages = (total_transactions + per_page - 1) // per_page

    # Paginated query of the transactions
    c.execute("""
        SELECT txid, SUM(amount) AS total_amount, block_hash, MAX(timestamp) AS timestamp, type
        FROM (
            SELECT t.txid, v.amount, v.block_hash, t.timestamp, 'received' AS type
            FROM vout v
            JOIN transactions t ON v.txid = t.txid
            WHERE v.address = ?
            UNION ALL
            SELECT i.txid, v.amount, v.block_hash, t.timestamp, 'sent' AS type
            FROM vin i
            JOIN vout v ON i.vout_txid = v.txid AND i.vout_index = v.ind
            JOIN transactions t ON i.txid = t.txid
            WHERE v.address = ? AND t.is_coinbase = 0
        ) AS combined
        GROUP BY txid, type
        ORDER BY MAX(timestamp) DESC
        LIMIT ? OFFSET ?
    """, (address, address, per_page, offset))
    transactions = [{
        'txid': tx['txid'],
        'amount': tx['total_amount'],
        'block_hash': tx['block_hash'],
        'timestamp': datetime.fromtimestamp(tx['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
        'type': tx['type']
    } for tx in c.fetchall()]

    conn.close()
    return render_template('address_detail.html', address=address, address_details={
        'total_received': total_received,
        'total_sent': total_sent,
        'balance': balance
    }, transactions=transactions, pages=range(1, total_pages + 1), page=page, total_pages=total_pages)


def paginate(current_page, total_pages, display_pages=5):
    # Generates a list of page numbers for pagination.
    half_window = display_pages // 2
    start_page = max(current_page - half_window, 1)
    end_page = min(current_page + half_window, total_pages)
    if end_page - start_page < display_pages - 1:
        start_page = max(end_page - display_pages + 1, 1)
        end_page = min(start_page + display_pages - 1, total_pages)
    return range(start_page, end_page + 1)


def get_current_difficulty():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        # Query the latest difficulty based on the highest block height
        c.execute('SELECT difficulty FROM blocks ORDER BY block_height DESC LIMIT 1')
        result = c.fetchone()
        return result[0] if result else None


def get_hash_rate(num_pow_blocks=20, use_target=False, target_pow_seconds=None):
    import sqlite3
    from math import isfinite

    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        # PoW blocks only
        c.execute("""
            SELECT timestamp, difficulty
            FROM blocks
            WHERE flags='proof-of-work'
            ORDER BY block_height DESC
            LIMIT ?
        """, (num_pow_blocks + 1,))
        rows = c.fetchall()

    if len(rows) < 2:
        return None

    cleaned = []
    for ts, diff in rows:
        try:
            ts = int(ts)
        except Exception:
            import datetime, time
            ts = int(time.mktime(datetime.datetime.fromisoformat(ts.replace(" UTC","")).timetuple()))
        diff = float(diff)
        cleaned.append((ts, diff))

    if use_target:
        if not target_pow_seconds or target_pow_seconds <= 0:
            raise ValueError("target_pow_seconds must be set for use_target=True")
        avg_diff = sum(d for _, d in cleaned[:-1]) / (len(cleaned) - 1)
        h = (avg_diff * (2**32)) / target_pow_seconds
    else:
        total_work = 0.0
        total_time = 0.0
        for i in range(1, len(cleaned)):
            ts_prev, _ = cleaned[i-1]
            ts_curr, d_curr = cleaned[i]
            dt = ts_prev - ts_curr
            if dt > 0 and isfinite(d_curr) and d_curr > 0:
                total_time += dt
                total_work += d_curr * (2**32)
        if total_time <= 0:
            return None
        h = total_work / total_time

    # kH/s
    return round(h / 1000.0, 2)


@app.route('/blockchain-stats')
def blockchain_stats():
    total_supply = calculate_total_supply()
    current_difficulty = get_current_difficulty()
    hash_rate = get_hash_rate()
    total_burnt = get_total_burnt()

    return render_template('blockchain_stats.html',
                           total_supply=round(total_supply, 2),
                           current_difficulty=current_difficulty,
                           hash_rate=hash_rate,
                           total_burnt=round(total_burnt, 2))


@app.route('/consensus-stats')
def consensus_stats():
    total_supply = calculate_total_supply() + get_total_burnt()
    current_difficulty = get_current_difficulty()
    hash_rate = get_hash_rate()
    total_burnt = get_total_burnt()
    percentage_burnt = total_burnt / total_supply * 100

    # Consensus breakdown over the last 100 blocks
    cs = consensus_stats_last_n(100)
    br = cs.get('breakdown', {})

    pow_pct = br.get('pow', {}).get('percentage', 0.0)
    pos_pct = br.get('pos', {}).get('percentage', 0.0)
    pob_pct = br.get('pob', {}).get('percentage', 0.0)
    other_pct = br.get('other', {}).get('percentage', 0.0)

    pow_avg = br.get('pow', {}).get('avg_reward', None)
    pos_avg = br.get('pos', {}).get('avg_reward', None)
    pob_avg = br.get('pob', {}).get('avg_reward', None)
    other_avg = br.get('other', {}).get('avg_reward', None)

    return render_template('consensus_stats.html',
                           total_supply=round(total_supply, 3),
                           current_difficulty=current_difficulty,
                           hash_rate=hash_rate,
                           total_burnt=round(total_burnt, 3),
                           consensus_window=cs.get('window_blocks', 0),
                           pow_pct=round(pow_pct, 1), pos_pct=round(pos_pct, 1), pob_pct=round(pob_pct, 1), other_pct=round(other_pct, 1),
                           pow_avg_reward=(round(pow_avg, 1) if pow_avg is not None else None),
                           pos_avg_reward=(round(pos_avg, 1) if pos_avg is not None else None),
                           pob_avg_reward=(round(pob_avg, 1) if pob_avg is not None else None),
                           other_avg_reward=(round(other_avg, 1) if other_avg is not None else None),
                           consensus_stats=cs,
                           percentage_burnt=round(percentage_burnt, 2))


@app.route('/search')
def search():
    query = request.args.get('query', '')
    query = re.sub(r'\W+', '', query)

    if query.isdigit():  # Search for block index
        block = search_block_by_index(query)
        if block:
            return redirect(url_for('block_detail', block_hash=block['block_hash'], _external=True, _scheme='http'))

    elif len(query) == 64:  # Length of a TX or Block hash
        tx = search_transaction(query)
        if tx:
            return redirect(url_for('transaction_detail', txid=query, _external=True, _scheme='http'))
        block = search_block_by_hash(query)
        if block:
            return redirect(url_for('block_detail', block_hash=query, _external=True, _scheme='http'))

    elif len(query) == 34:  # Length of a Bitcoin-based address
        address = search_address(query)
        if address:
            return redirect(url_for('address_detail', address=query, _external=True, _scheme='http'))

    # If nothing is found, show a message or reload the start page
    return render_template('not_found.html'), 404


@app.route('/richlist')
def rich_list():
    conn = get_db_connection()
    c = conn.cursor()
    total_supply = calculate_total_supply()  # Use your existing function to calculate the total supply

    c.execute("""
        SELECT address, balance AS total_amount, (balance / ? * 100) AS percentage
        FROM addresses
        ORDER BY total_amount DESC
        LIMIT 100
    """, (total_supply,))
    wallets = c.fetchall()
    conn.close()

    return render_template('rich_list.html', wallets=wallets, total_supply=total_supply)


@app.route('/network')
def network():
    peers = get_peers()  # Retrieve the list of all peers from the database
    return render_template('network.html', peers=peers)  # Transfer the peers to the template


def search_block_by_index(index):
    with sqlite3.connect(DATABASE) as conn:
        conn.row_factory = sqlite3.Row  # Make sure that the results are returned as row dictionaries
        c = conn.cursor()
        c.execute('SELECT * FROM blocks WHERE block_height = ?', (index,))
        block = c.fetchone()
        return block


def search_block_by_hash(hash):
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM blocks WHERE block_hash = ?', (hash,))
        return c.fetchone()


def search_transaction(txid):
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM transactions WHERE txid = ?', (txid,))
        return c.fetchone()


def search_address(address):
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM addresses WHERE address = ?', (address,))
        return c.fetchone()


def get_peers():
    with sqlite3.connect(PEERS) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM peers')  # Query for all peers
        return c.fetchall()  # Returns a list of all peers


@app.route('/api/total_supply')
def total_supply_api():
    total_supply = calculate_total_supply()  # This function retrieves the current Total Supply from the database
    return jsonify({
        'total_supply': total_supply,
        'unit': 'HABS'
    })


# New JSON API route for block meta
@app.route('/api/block_meta/<block_hash>')
def block_meta(block_hash):
    with sqlite3.connect(DATABASE) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT 
                block_height, block_hash, timestamp, confirmations, difficulty, nonce, prev_hash,
                flags, effective_burn_coins, mint, burnt
            FROM blocks
            WHERE block_hash = ?
        """, (block_hash,))
        row = c.fetchone()
        if not row:
            return jsonify({'ok': False, 'error': 'block not found'}), 404
        return jsonify({
            'ok': True,
            'block_height': row['block_height'],
            'block_hash': row['block_hash'],
            'timestamp': row['timestamp'],
            'confirmations': row['confirmations'],
            'difficulty': row['difficulty'],
            'nonce': row['nonce'],
            'prev_hash': row['prev_hash'],
            'flags': row['flags'],
            'effective_burn_coins': row['effective_burn_coins'],
            'mint': row['mint'],
            'burnt': row['burnt'],
        })


# Helper to compute consensus breakdowns over the last N blocks

def consensus_stats_last_n(num_blocks=100):
    num_blocks = max(1, int(num_blocks))
    with sqlite3.connect(DATABASE) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT flags, COALESCE(mint, 0) AS mint
            FROM blocks
            ORDER BY block_height DESC
            LIMIT ?
        """, (num_blocks,))
        rows = c.fetchall()
        total = len(rows)
        if total == 0:
            return {
                'ok': True,
                'window_blocks': 0,
                'breakdown': {
                    'pow': {'count': 0, 'percentage': 0.0, 'avg_reward': None},
                    'pos': {'count': 0, 'percentage': 0.0, 'avg_reward': None},
                    'pob': {'count': 0, 'percentage': 0.0, 'avg_reward': None},
                    'other': {'count': 0, 'percentage': 0.0, 'avg_reward': None}
                }
            }

        key_map = {
            'proof-of-work': 'pow',
            'proof-of-stake': 'pos',
            'proof-of-burn': 'pob'
        }
        counts = {'pow': 0, 'pos': 0, 'pob': 0, 'other': 0}
        sums = {'pow': 0.0, 'pos': 0.0, 'pob': 0.0, 'other': 0.0}

        for r in rows:
            flag = (r['flags'] or '').strip().lower()
            k = key_map.get(flag, 'other')
            counts[k] += 1
            sums[k] += float(r['mint'] or 0.0)

        def pct(x):
            return round((x / total) * 100.0, 2)

        def avg(s, c):
            return round(s / c, 4) if c else None

        return {
            'ok': True,
            'window_blocks': total,
            'breakdown': {
                'pow': {'count': counts['pow'], 'percentage': pct(counts['pow']),
                        'avg_reward': avg(sums['pow'], counts['pow'])},
                'pos': {'count': counts['pos'], 'percentage': pct(counts['pos']),
                        'avg_reward': avg(sums['pos'], counts['pos'])},
                'pob': {'count': counts['pob'], 'percentage': pct(counts['pob']),
                        'avg_reward': avg(sums['pob'], counts['pob'])},
                'other': {'count': counts['other'], 'percentage': pct(counts['other']),
                          'avg_reward': avg(sums['other'], counts['other'])}
            }
        }


# New JSON API route for consensus stats
@app.route('/api/consensus_stats')
def consensus_stats_api():
    try:
        last = request.args.get('last', default=100, type=int)
    except Exception:
        last = 100
    stats = consensus_stats_last_n(last)
    return jsonify(stats)


if __name__ == '__main__':
    app.run(debug=False)
