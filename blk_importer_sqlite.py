#!/usr/bin/env python3
"""
Slimcoin blk*.dat -> blockchain.db importer

- Uses the same DB schema as database.py (blocks, transactions, vin, vout, addresses)
- Uses reinitialize_tables(), drop_indices(), create_indices(), rebuild_addresses()
  from database.py
- Reads Slimcoin raw blocks directly from blkNNNN.dat files
- Txid hashing: Peercoin/Slimcoin include nTime in the txid when present
- No RPC, no JSON blocks
"""

import os
import sys
import struct
import sqlite3
import hashlib
from io import BytesIO
from dcrypt_pure import dcrypt_pow_hash
from multiprocessing import Pool, cpu_count


# Ensure the directory of this script is on sys.path so that "database" imports work
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# Import from your explorer
from database import (
    DATABASE,
    reinitialize_tables,
    drop_indices,
    create_indices,
    rebuild_addresses,
)

# Slimcoin mainnet magic (pchMessageStart)
NETWORK_MAGIC = b"\x6e\x8b\x92\xa5"

# Slimcoin uses 10^8 units per coin (like Bitcoin)
COIN = 100_000_000

# Base58 alphabet (Bitcoin-style)
B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# Slimcoin P2PKH address prefix (0x3f -> addresses starting with "S")
P2PKH_PREFIX = 0x3F

# Global: controls how we interpret block header hashes / prev-hashes.
# True  -> Bitcoin-style: double-SHA256(header)[::-1].hex(), prev_hash bytes[::-1].hex()
# False -> "Raw" little-endian hex: double-SHA256(header).hex(), prev_hash bytes.hex()
USE_REVERSED_HASH = True


# ----------- helper functions -----------

def read_varint(f: BytesIO) -> int:
    b = f.read(1)
    if not b:
        raise EOFError("Unexpected EOF while reading varint")
    n = b[0]
    if n < 0xFD:
        return n
    if n == 0xFD:
        return struct.unpack("<H", f.read(2))[0]
    if n == 0xFE:
        return struct.unpack("<I", f.read(4))[0]
    return struct.unpack("<Q", f.read(8))[0]

# --- helper to write varints, for canonical txid construction ---
def write_varint(n: int) -> bytes:
    if n < 0xFD:
        return bytes([n])
    if n <= 0xFFFF:
        return b"\xFD" + struct.pack("<H", n)
    if n <= 0xFFFFFFFF:
        return b"\xFE" + struct.pack("<I", n)
    return b"\xFF" + struct.pack("<Q", n)


def hash256(b: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def hash160(b: bytes) -> bytes:
    sha = hashlib.sha256(b).digest()
    ripe = hashlib.new("ripemd160", sha).digest()
    return ripe

# --- Merkle root computation (Bitcoin-style, double-SHA256) ---
def compute_merkle_root_hex(txids):
    """
    Compute the Merkle root (Bitcoin-style, double-SHA256) of a list of txids.

    txids: list of 64-char hex strings in RPC (big-endian) form.
    Returns the Merkle root as 64-char hex string in RPC (big-endian) form.
    """
    if not txids:
        # Slimcoin blocks should always have at least one transaction,
        # but return zero root as a safeguard.
        return "0" * 64

    # Convert to little-endian bytes for internal Merkle computation
    hashes = [bytes.fromhex(txid)[::-1] for txid in txids]

    while len(hashes) > 1:
        if len(hashes) % 2 == 1:
            # If odd number of elements, duplicate the last one
            hashes.append(hashes[-1])
        new_level = []
        for i in range(0, len(hashes), 2):
            new_level.append(hash256(hashes[i] + hashes[i + 1]))
        hashes = new_level

    # Final Merkle root as little-endian; convert back to big-endian hex
    root_le = hashes[0]
    return root_le[::-1].hex()


def b58encode(b: bytes) -> str:
    # count leading zeros (-> "1" prefix in Base58)
    n_zeros = len(b) - len(b.lstrip(b"\x00"))
    x = int.from_bytes(b, "big")
    out = bytearray()
    while x > 0:
        x, rem = divmod(x, 58)
        out.append(B58_ALPHABET[rem])
    out.extend(b"1" * n_zeros)
    out.reverse()
    return out.decode("ascii")


# --- Safe integer readers with context-aware error messages ---
def read_u32(f: BytesIO, context: str) -> int:
    """Read a little-endian unsigned 32-bit int with a helpful error on short read."""
    pos = f.tell()
    b = f.read(4)
    if len(b) != 4:
        raise ValueError(f"Short read ({len(b)} bytes) for u32 at offset {pos} while parsing {context}")
    return struct.unpack("<I", b)[0]


def read_i64(f: BytesIO, context: str) -> int:
    """Read a little-endian signed 64-bit int with a helpful error on short read."""
    pos = f.tell()
    b = f.read(8)
    if len(b) != 8:
        raise ValueError(f"Short read ({len(b)} bytes) for i64 at offset {pos} while parsing {context}")
    return struct.unpack("<q", b)[0]


# --- parse a single transaction with optional nTime (Peercoin/Slimcoin style) ---
def parse_tx(f: BytesIO, allow_time_field: bool):
    """Parse a single Slimcoin transaction from the byte stream.

    Disk layout variants we support:
    - Bitcoin-style:  version, vin, vout, locktime
    - Peercoin-style: version, nTime, vin, vout, locktime

    `allow_time_field=True` means we expect an extra 4-byte nTime after the
    version. **Important:** In Peercoin-derived chains (including Slimcoin),
    the transaction hash (txid) includes this `nTime` field. Therefore, if
    `nTime` is present we serialize it between `version` and the vin list
    when computing the txid.
    """
    tx_start = f.tell()

    v = read_u32(f, "tx.version")
    n_time = None

    # Peercoin/Slimcoin-style: when allow_time_field=True we expect an extra
    # 4-byte nTime field after the version. The caller (parse_block_transactions)
    # will try both with and without this field and choose the layout that fits
    # the block body best.
    if allow_time_field:
        n_time = read_u32(f, "tx.time")

    vin_count = read_varint(f)

    vins = []
    for _ in range(vin_count):
        prev_txid_le = f.read(32)
        if len(prev_txid_le) != 32:
            raise ValueError("Short read for vin.prev_txid")
        prev_txid = prev_txid_le[::-1].hex()
        vout_index = read_u32(f, "vin.vout_index")
        script_len = read_varint(f)
        script_sig = f.read(script_len)
        if len(script_sig) != script_len:
            raise ValueError("Short read for vin.script_sig")
        sequence = read_u32(f, "vin.sequence")
        vins.append({
            "prev_txid": prev_txid,
            "vout_index": vout_index,
            "script_sig": script_sig,
            "sequence": sequence,
        })

    vout_count = read_varint(f)
    vouts = []
    for vout_n in range(vout_count):
        value_satoshi = read_i64(f, "vout.value_satoshi")
        pk_script_len = read_varint(f)
        pk_script = f.read(pk_script_len)
        if len(pk_script) != pk_script_len:
            raise ValueError("Short read for vout.pk_script")
        vouts.append({
            "n": vout_n,
            "value_satoshi": value_satoshi,
            "pk_script": pk_script,
        })

    locktime = read_u32(f, "tx.locktime")
    tx_end = f.tell()

    # Reconstruct bytes for txid hashing:
    # Peercoin/Slimcoin semantics: if nTime is present, include it in the hash
    from io import BytesIO as _BytesIO
    buf = _BytesIO()
    buf.write(struct.pack("<I", v))
    if n_time is not None:
        buf.write(struct.pack("<I", n_time))
    buf.write(write_varint(vin_count))
    for vin in vins:
        buf.write(bytes.fromhex(vin["prev_txid"])[::-1])
        buf.write(struct.pack("<I", vin["vout_index"]))
        script_sig = vin["script_sig"]
        buf.write(write_varint(len(script_sig)))
        buf.write(script_sig)
        buf.write(struct.pack("<I", vin["sequence"]))
    buf.write(write_varint(vout_count))
    for vout in vouts:
        buf.write(struct.pack("<q", vout["value_satoshi"]))
        pk_script = vout["pk_script"]
        buf.write(write_varint(len(pk_script)))
        buf.write(pk_script)
    buf.write(struct.pack("<I", locktime))
    tx_bytes = buf.getvalue()

    txid = hash256(tx_bytes)[::-1].hex()

    # Coinbase detection: prev_txid = 0...0 and vout_index = 0xffffffff
    is_coinbase = False
    if vins:
        first_vin = vins[0]
        if first_vin["prev_txid"] == "0" * 64 and first_vin["vout_index"] == 0xFFFFFFFF:
            is_coinbase = True

    tx = {
        "txid": txid,
        "version": v,
        "time": n_time,
        "locktime": locktime,
        "vin": vins,
        "vout": vouts,
        "is_coinbase": is_coinbase,
    }

    return tx, tx_end


def p2pkh_address_from_hash160(h160: bytes) -> str:
    payload = bytes([P2PKH_PREFIX]) + h160
    checksum = hash256(payload)[:4]
    return b58encode(payload + checksum)


def extract_address_from_pk_script(pk_script: bytes) -> str:
    """
    Currently only supports standard P2PKH:
      OP_DUP OP_HASH160 <20-byte> OP_EQUALVERIFY OP_CHECKSIG
      hex: 76 a9 14 <20-byte> 88 ac
    Returns empty string if script is not supported.
    """
    if (
        len(pk_script) == 25
        and pk_script[0] == 0x76
        and pk_script[1] == 0xA9
        and pk_script[2] == 0x14
        and pk_script[-2] == 0x88
        and pk_script[-1] == 0xAC
    ):
        h160 = pk_script[3:23]
        return p2pkh_address_from_hash160(h160)
    return ""


def difficulty_from_bits(bits: int, diff1_target: int) -> float:
    """
    Approximate Bitcoin-style difficulty:
      target = mantissa * 2^(8*(exponent-3))
      difficulty = diff1_target / target
    diff1_target is derived from the genesis bits.
    """
    exponent = bits >> 24
    mantissa = bits & 0xFFFFFF
    if mantissa == 0:
        return 0.0
    target = mantissa * (1 << (8 * (exponent - 3)))
    if target == 0:
        return 0.0
    return float(diff1_target / target)


def parse_block_header_bytes(header: bytes, reversed_hash: bool):
    """
    Parse the 80-byte header and compute Slimcoin block hash / prev-hash.

    - Blockhash: dcrypt_pow_hash(header)[::-1].hex()  (RPC-Stil)
    - Prev-hash: Headerfeld als little-endian, also raw_prev[::-1].hex()
    """
    if len(header) < 80:
        raise ValueError("Block header too short")

    version = struct.unpack("<I", header[:4])[0]
    raw_prev = header[4:36]
    raw_merkle = header[36:68]
    timestamp, bits, nonce = struct.unpack("<III", header[68:80])

    # Slimcoin PoW: Dcrypt über den *Header*, dann für RPC-Stil byteswappen
    pow_bytes = dcrypt_pow_hash(header)       # 32 Bytes, big-endian
    block_hash = pow_bytes[::-1].hex()        # RPC / Explorer / getblockhash-Stil

    # Prev-/Merkle-Hash sind klassische uint256-Felder im Header
    prev_block = raw_prev[::-1].hex()
    merkle_root = raw_merkle[::-1].hex()

    return {
        "hash": block_hash,
        "version": version,
        "prev_block": prev_block,
        "merkle_root": merkle_root,
        "time": timestamp,
        "bits": bits,
        "nonce": nonce,
    }


# ----------- block parsing -----------

def parse_block_header(block_bytes: bytes):
    """
    Parse the 80-byte header and compute block hash, using Slimcoin Dcrypt PoW.
    """
    header = block_bytes[:80]
    return parse_block_header_bytes(header, USE_REVERSED_HASH)


# --- robust transaction parser for Slimcoin blocks (handles PoB extra data, validates Merkle root) ---
def parse_block_transactions(block_bytes: bytes, expected_merkle: str):
    """Parse all transactions from a Slimcoin block body.

    Some Slimcoin blocks (e.g. proof-of-burn or other special cases) can carry
    extra data before or after the transaction list. We therefore:
      - try multiple candidate offsets ("skip" bytes) between the 80-byte
        header and the tx-count varint,
      - try both tx layouts (with and without nTime),
      - for each candidate, compute the Merkle root from parsed txids and
        require it to match the header's merkle_root,
      - among all matching candidates, choose the one that leaves the
        smallest unused tail in the block body.

    This Merkle-root check makes the transaction parsing robust against
    layout ambiguities and ensures that txids match the block header.
    """
    if len(block_bytes) <= 80:
        return []

    body = block_bytes[80:]
    last_error = None

    best_txs = None
    best_remaining = None

    # Candidate offsets up to 2048 bytes; normal blocks succeed at skip=0.
    max_skip = min(2048, len(body))
    for skip in range(0, max_skip + 1):
        for allow_time in (True, False):
            f = BytesIO(body[skip:])
            try:
                tx_count = read_varint(f)
                # basic sanity guard: Slimcoin blocks never have anywhere close to this
                if tx_count > 20000:
                    continue
                txs = []
                for tx_index in range(tx_count):
                    tx, _ = parse_tx(f, allow_time_field=allow_time)
                    tx["index_in_block"] = tx_index
                    txs.append(tx)

                consumed = f.tell()
                remaining = len(body) - skip - consumed
                if remaining < 0:
                    # We read past the end; this variant is invalid
                    continue

                # Compute Merkle root from parsed txids and compare to header
                txids = [tx["txid"] for tx in txs]
                merkle_hex = compute_merkle_root_hex(txids)
                if merkle_hex != expected_merkle:
                    # Layout does not match header's Merkle root
                    continue

                # Prefer variants that leave less unparsed data at the end.
                if best_txs is None or remaining < best_remaining:
                    best_txs = txs
                    best_remaining = remaining

                    # Perfect match: no remaining bytes -> stop searching.
                    if best_remaining == 0:
                        return best_txs

            except Exception as e:
                last_error = e
                continue

    if best_txs is not None:
        # At least one variant was found that matched the Merkle root and
        # did not overrun the buffer. Accept the best match even if some
        # trailing bytes remain unparsed (assumed to be Slimcoin-specific
        # metadata).
        return best_txs

    # If we get here, no variant succeeded; raise the last error for context
    if last_error is not None:
        raise last_error
    raise ValueError("Could not parse block transactions (empty or invalid block body)")


def parse_full_block(block_bytes: bytes):
    """
    Parse header + transactions.
    Returns a block dict and list of tx dicts.
    """
    header = block_bytes[:80]
    if len(header) != 80:
        raise ValueError("Short block header")

    hdr = parse_block_header_bytes(header, USE_REVERSED_HASH)
    block_hash = hdr["hash"]
    version = hdr["version"]
    prev_block = hdr["prev_block"]
    merkle_root = hdr["merkle_root"]
    timestamp = hdr["time"]
    bits = hdr["bits"]
    nonce = hdr["nonce"]

    txs = parse_block_transactions(block_bytes, merkle_root)

    return {
        "hash": block_hash,
        "version": version,
        "prev_block": prev_block,
        "merkle_root": merkle_root,
        "time": timestamp,
        "bits": bits,
        "nonce": nonce,
        "txs": txs,
    }


def iter_block_bytes(blocks_dir):
    """
    Generator over all block payloads in blkNNNN.dat files.
    Yields (block_bytes, block_size).
    """
    for name in sorted(os.listdir(blocks_dir)):
        if not (name.startswith("blk") and name.endswith(".dat")):
            continue
        middle = name[3:-4]
        if not middle.isdigit():
            continue
        path = os.path.join(blocks_dir, name)
        with open(path, "rb") as f:
            while True:
                magic = f.read(4)
                if not magic:
                    break
                if magic != NETWORK_MAGIC:
                    raise ValueError(f"Unexpected magic {magic.hex()} in {path}")
                size_bytes = f.read(4)
                if len(size_bytes) != 4:
                    break
                (block_size,) = struct.unpack("<I", size_bytes)
                block_bytes = f.read(block_size)
                if len(block_bytes) != block_size:
                    break
                yield block_bytes, block_size


# ----------- iter_block_headers: header-only iterator -----------
def iter_block_headers(blocks_dir):
    """
    Generator over all block headers in blkNNNN.dat files.
    Yields (header80, block_size).
    Reads only the 80-byte header, skips the rest of the block payload.
    """
    for name in sorted(os.listdir(blocks_dir)):
        if not (name.startswith("blk") and name.endswith(".dat")):
            continue
        middle = name[3:-4]
        if not middle.isdigit():
            continue
        path = os.path.join(blocks_dir, name)
        with open(path, "rb") as f:
            while True:
                magic = f.read(4)
                if not magic:
                    break
                if magic != NETWORK_MAGIC:
                    raise ValueError(f"Unexpected magic {magic.hex()} in {path}")
                size_bytes = f.read(4)
                if len(size_bytes) != 4:
                    break
                (block_size,) = struct.unpack("<I", size_bytes)
                if block_size < 80:
                    raise ValueError(f"Block size {block_size} too small for header in {path}")
                header80 = f.read(80)
                if len(header80) != 80:
                    break
                # Skip the remaining payload if any
                if block_size > 80:
                    f.seek(block_size - 80, os.SEEK_CUR)
                yield header80, block_size


# ----------- main-chain discovery (pass 1) -----------


# ----------- multiprocessing header parser helper -----------
def _parse_header_for_pool(args):
    """
    Helper for multiprocessing: parse a single block header in a worker process.
    Args:
        args: tuple (header80, block_size)
    Returns:
        ("ok", hash, prev_block, time, bits, block_size, None) on success
        ("err", None, None, None, None, block_size, error_message) on failure
    """
    header80, block_size = args
    try:
        h = parse_block_header_bytes(header80, True)
        return ("ok", h["hash"], h["prev_block"], h["time"], h["bits"], block_size, None)
    except Exception as e:
        return ("err", None, None, None, None, block_size, str(e))


from collections import defaultdict, deque

def _build_chain_for_headers(headers):
    """Compute heights, main chain and diff1_target from a headers dict.

    headers format:
      {block_hash: {"prev": ..., "time": ..., "bits": ..., "size": ...}}

    Returns:
      height_map:  {block_hash: height}
      main_chain:  set of block hashes on the selected main chain
      tip_hash:    hash of the highest block
      tip_height:  height of the highest block
      diff1_target: target for difficulty=1 derived from genesis bits
    """
    height_map = {}
    diff1_target = None

    # Build forward adjacency (parent -> list of children) and find genesis candidates
    children = defaultdict(list)
    genesis_candidates = set()

    for bh, meta in headers.items():
        prev = meta["prev"]
        if prev and prev in headers:
            children[prev].append(bh)
        else:
            # No known parent in this header set -> treat as genesis/root
            genesis_candidates.add(bh)

    if not genesis_candidates:
        raise RuntimeError("No genesis candidates found when building chain heights")

    # BFS / level-order traversal starting from all genesis blocks
    q = deque()
    for g in genesis_candidates:
        height_map[g] = 0
        q.append(g)
        if diff1_target is None:
            bits = headers[g]["bits"]
            exponent = bits >> 24
            mantissa = bits & 0xFFFFFF
            diff1_target = mantissa * (1 << (8 * (exponent - 3)))

    processed = 0
    total = len(headers)

    while q:
        cur = q.popleft()
        processed += 1
        if processed % 50000 == 0 or processed == total:
            print(f"[pass1]   computed heights for {processed}/{total} headers...", flush=True)

        cur_height = height_map[cur]
        for child in children.get(cur, []):
            # If we already have a height for this child, keep the first one
            # (there should be no alternative shorter path in a proper chain).
            if child in height_map:
                continue
            height_map[child] = cur_height + 1
            q.append(child)

    if not height_map:
        raise RuntimeError("No block heights computed (empty headers?)")

    # Select tip as the block with the maximum height
    tip_hash = max(height_map.keys(), key=lambda bh: height_map[bh])
    tip_height = height_map[tip_hash]

    # Walk back from tip to genesis to obtain the main chain set
    main_chain = set()
    visited_backwards = set()
    cur = tip_hash
    while True:
        if cur in visited_backwards:
            raise RuntimeError("Cycle detected while walking back main chain from tip")
        visited_backwards.add(cur)
        main_chain.add(cur)
        prev = headers[cur]["prev"]
        if not prev or prev not in headers:
            break
        cur = prev

    if diff1_target is None:
        # Fallback, should not normally happen
        bits = headers[tip_hash]["bits"]
        exponent = bits >> 24
        mantissa = bits & 0xFFFFFF
        diff1_target = mantissa * (1 << (8 * (exponent - 3)))

    return height_map, main_chain, tip_hash, tip_height, diff1_target


def build_chain_headers(blocks_dir, workers=1):
    """
    Pass 1:
      - scan all headers from blk*.dat
      - use only reversed hashes (Bitcoin-style)
      - compute heights and determine main chain
      Optionally uses multiple worker processes for Dcrypt header hashing.
    Returns:
      headers:    {block_hash: {prev, time, bits, size}}
      height_map: {block_hash: height}
      main_chain: set(block_hash) for main chain only
      tip_hash, tip_height
      diff1_target: target for difficulty=1 derived from genesis bits
    """
    global USE_REVERSED_HASH
    USE_REVERSED_HASH = True
    headers = {}
    header_count = 0
    print("[pass1] Scanning blk*.dat headers...")
    if workers is None or workers <= 1:
        # Single-process
        for header80, block_size in iter_block_headers(blocks_dir):
            header_count += 1
            if header_count % 10000 == 0:
                print(f"[pass1]   scanned {header_count} block headers so far...", flush=True)
            if len(header80) < 80:
                continue
            h = parse_block_header_bytes(header80, True)
            headers[h["hash"]] = {
                "prev": h["prev_block"],
                "time": h["time"],
                "bits": h["bits"],
                "size": block_size,
            }
    else:
        # Multi-process
        num_workers = min(max(1, workers), cpu_count() or 1)
        print(f"[pass1] Using {num_workers} worker processes for header Dcrypt...", flush=True)
        from multiprocessing import Pool
        pool = Pool(processes=num_workers)
        try:
            for result in pool.imap_unordered(_parse_header_for_pool, iter_block_headers(blocks_dir), chunksize=200):
                status, bh, prev, t, bits, block_size, err_msg = result
                header_count += 1
                if header_count % 10000 == 0:
                    print(f"[pass1]   scanned {header_count} block headers so far...", flush=True)
                if status != "ok":
                    print(f"[warn] Failed to parse header: {err_msg}")
                    continue
                headers[bh] = {
                    "prev": prev,
                    "time": t,
                    "bits": bits,
                    "size": block_size,
                }
        finally:
            pool.close()
            pool.join()
    if not headers:
        raise RuntimeError("No blocks found in blk*.dat")
    height_map, main_chain, tip_hash, tip_height, diff1_target = _build_chain_for_headers(headers)
    print(f"[pass1] Main chain tip height {tip_height}, tip hash {tip_hash}")
    print(f"[pass1] Found {len(headers)} blocks in files.")
    return headers, height_map, main_chain, tip_hash, tip_height, diff1_target


# ----------- import into your DB (pass 2) -----------


# Helper for multiprocessing block parsing
def _parse_block_for_pool(args):
    """
    Helper for multiprocessing: parse a single block in a worker process.

    Args:
        args: tuple (block_bytes, block_size)

    Returns:
        ("ok", block_dict, block_size, None, header80) on success
        ("err", None, block_size, error_message, header80) on failure
    """
    block_bytes, block_size = args
    header80 = block_bytes[:80]
    try:
        block = parse_full_block(block_bytes)
        return ("ok", block, block_size, None, header80)
    except Exception as e:
        # Return the error message as string so the main process can log it
        return ("err", None, block_size, str(e), header80)


# ----------- post-processing helpers (spent flags & addresses) -----------

BATCH_SIZE_SPENT = 10000  # number of vin rows per batch when rebuilding spent flags


def ensure_link_indices(conn):
    """
    Ensure we have helper indices to efficiently link vin -> vout.
    These indices are cheap to create and speed up spent-flag rebuilding.
    They are kept in addition to the normal explorer indices.
    """
    c = conn.cursor()
    # Index on vin(vout_txid, vout_index)
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_vin_outref "
        "ON vin(vout_txid, vout_index)"
    )
    # Index on vout(txid, ind)
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_vout_outref "
        "ON vout(txid, ind)"
    )
    conn.commit()


def rebuild_spent_flags(conn):
    """
    Reset and rebuild vout.spent in small batches based on vin.

    Logic:
      - Set all vout.spent = 0
      - Iterate over vin rows in rowid order in batches.
      - For each vin, mark the referenced vout(txid, ind) as spent=1.

    This is designed to be memory-friendly and can be re-run if needed.
    """
    c = conn.cursor()

    print("[spent] Ensuring helper indices for vin/vout linking...")
    ensure_link_indices(conn)

    print("[spent] Resetting all vout.spent to 0...")
    c.execute("UPDATE vout SET spent = 0")
    conn.commit()

    print("[spent] Rebuilding spent flags from vin in batches...")
    last_rowid = 0
    total_processed = 0

    while True:
        # Fetch next batch of vin rows
        c.execute(
            """
            SELECT rowid, vout_txid, vout_index
            FROM vin
            WHERE rowid > ?
            ORDER BY rowid
            LIMIT ?
            """,
            (last_rowid, BATCH_SIZE_SPENT),
        )
        rows = c.fetchall()
        if not rows:
            break

        # Update corresponding vout rows
        for rowid, vout_txid, vout_index in rows:
            c.execute(
                "UPDATE vout SET spent = 1 WHERE txid = ? AND ind = ?",
                (vout_txid, int(vout_index)),
            )
            last_rowid = rowid
            total_processed += 1

        conn.commit()
        print(f"[spent] Processed {total_processed} vin rows so far...")

    print(f"[spent] Done. Total vin rows processed: {total_processed}")


 # --- de-duplicate helpers -----------------------------------------------------

def dedupe_vout(conn):
    """Remove duplicate rows from vout based on the (txid, ind) pair.
    Keeps the lowest rowid per pair. Necessary because during bulk import we
    temporarily drop indices; without a UNIQUE constraint, duplicates could be
    inserted and later cause the UNIQUE index creation to fail.
    """
    c = conn.cursor()
    # Measure before
    c.execute("SELECT COUNT(*) FROM vout")
    total_before = c.fetchone()[0]

    # Delete all duplicates, keeping the first (smallest rowid)
    c.execute(
        """
        DELETE FROM vout
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM vout
            GROUP BY txid, ind
        )
        """
    )
    conn.commit()

    # Report after
    c.execute("SELECT COUNT(*) FROM vout")
    total_after = c.fetchone()[0]
    removed = max(0, int(total_before) - int(total_after))
    print(f"[post] De-duplicated vout: removed {removed} rows (now {total_after}).")


def run_postprocessing(conn, tip_height: int):
    """
    Run all post-import steps on an existing explorer database connection:

      - Update confirmations based on the given tip_height
      - Rebuild vout.spent flags from vin in a memory-friendly way
      - Recreate explorer indices
      - Rebuild the addresses table up to tip_height

    This helper is used both after a fresh import and in post-only mode.
    """
    c = conn.cursor()

    # 1) Set confirmations in bulk based on true tip height
    print("[post] Updating confirmations...")
    c.execute(
        "UPDATE blocks SET confirmations = ? - block_height + 1",
        (int(tip_height),),
    )
    conn.commit()

    # 2) Rebuild spent flags from vin in batches
    print("[post] Rebuilding spent flags from vin...")
    rebuild_spent_flags(conn)

    # 3) Ensure no duplicate (txid, ind) before creating UNIQUE index
    print("[post] De-duplicating vout (txid, ind) before creating indices...")
    dedupe_vout(conn)

    # 4) Recreate indices
    print("[post] Creating indices...")
    create_indices(conn)
    conn.commit()

    # 5) Rebuild addresses table from vout
    print("[post] Rebuilding addresses table...")
    rebuild_addresses(conn, int(tip_height))
    print("[done] Post-processing finished.")


def import_mainchain_to_db(blocks_dir, workers=1):
    """
    Import main-chain blocks into the explorer database.

    Steps:
      1) First pass: build chain headers, heights, and main-chain set
         (optionally multi-core, controlled by 'workers')
      2) Second pass: iterate blk*.dat again, parse blocks
         - ALWAYS single-process to keep memory usage low
         - skip orphan/side-chain blocks
         - insert only main-chain blocks with their true height
    """
    # ---------- PASS 1: discover main chain ----------
    (
        headers,
        height_map,
        main_chain,
        tip_hash,
        tip_height,
        diff1_target,
    ) = build_chain_headers(blocks_dir, workers=workers)

    print(
        f"[pass1] Main chain tip height {tip_height}, tip hash {tip_hash}, "
        f"{len(main_chain)} main-chain blocks out of {len(headers)} total."
    )

    print("[pass2] Importing main-chain blocks into SQLite (single-process)...")

    conn = sqlite3.connect(DATABASE, timeout=10)
    try:
        # Performance tuning pragmas similar to reindex_db
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-20000")
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("PRAGMA busy_timeout=5000")

        drop_indices(conn)

        # Enforce (txid, ind) uniqueness during import to prevent duplicates
        print("[pass2] Ensuring UNIQUE(vout.txid, vout.ind) during import...")
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_vout_txn ON vout(txid, ind)")
        except sqlite3.IntegrityError:
            # If duplicates already exist from a previous partial run, clean them once
            print("[pass2] Detected pre-existing duplicates in vout; de-duplicating once...")
            dedupe_vout(conn)
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_vout_txn ON vout(txid, ind)")
        conn.commit()

        c = conn.cursor()

        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")

        imported_blocks = 0
        seq_index = 0  # sequential position in blk*.dat (for logging only)

        # PASS 3: always single-process, streaming blocks directly from disk
        block_iter = iter_block_bytes(blocks_dir)

        for block_bytes, block_size in block_iter:
            header80 = block_bytes[:80]

            # Parse header first to decide whether we care about this block
            try:
                hdr = parse_block_header_bytes(header80, USE_REVERSED_HASH)
            except Exception as e_hdr:
                print(
                    f"[warn] Failed to parse header at sequential index {seq_index}: {e_hdr}"
                )
                seq_index += 1
                continue

            bh = hdr["hash"]
            bits = hdr["bits"]
            b_time = hdr["time"]

            # Skip orphan / side-chain blocks as early as possible
            if bh not in main_chain:
                seq_index += 1
                continue

            # Now parse full block including transactions
            try:
                block = parse_full_block(block_bytes)
            except Exception as e:
                print(
                    f"[warn] Failed to parse MAIN-CHAIN block at sequential index {seq_index}, "
                    f"hash {bh}, time {b_time}, bits {bits}: {e}"
                )
                seq_index += 1
                continue

            b_height = height_map[bh]

            # --- At this point we have a valid MAIN-CHAIN block dict + header info ---
            # Sanity: ensure block["hash"] matches header hash
            if block["hash"] != bh:
                print(
                    f"[warn] Hash mismatch for main-chain block at height {b_height}: "
                    f"header {bh} vs block {block['hash']}"
                )

            difficulty = difficulty_from_bits(bits, diff1_target)

            # Insert block row
            c.execute(
                """
                INSERT OR REPLACE INTO blocks
                  (block_hash, confirmations, block_size, block_height,
                   timestamp, nonce, difficulty, prev_hash, flags,
                   effective_burn_coins, mint, burnt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bh,
                    0,  # confirmations will be set later in one bulk update
                    int(block_size),
                    int(b_height),
                    int(b_time),
                    int(block["nonce"]),
                    float(difficulty),
                    block["prev_block"],
                    None,  # flags (PoW/PoS/PoB) can be added later
                    0.0,   # effective_burn_coins
                    0.0,   # mint
                    0.0,   # burnt
                ),
            )

            # Insert transactions
            for tx in block["txs"]:
                txid = tx["txid"]
                is_coinbase = tx["is_coinbase"]

                total_amount_slm = 0.0
                for vout in tx["vout"]:
                    total_amount_slm += vout["value_satoshi"] / COIN

                c.execute(
                    """
                    INSERT OR REPLACE INTO transactions
                      (txid, block_hash, amount, timestamp, is_coinbase)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        txid,
                        bh,
                        float(total_amount_slm),
                        int(b_time),
                        int(is_coinbase),
                    ),
                )

                # vout: outputs
                for vout in tx["vout"]:
                    vout_index = vout["n"]
                    value_slm = vout["value_satoshi"] / COIN
                    pk_script = vout["pk_script"]
                    address = extract_address_from_pk_script(pk_script) or ""

                    c.execute(
                        """
                        INSERT OR IGNORE INTO vout
                          (txid, ind, amount, address, spent, block_hash, created_block_height)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            txid,
                            vout_index,
                            float(value_slm),
                            address,
                            0,      # spent will be set later from vin
                            bh,
                            int(b_height),
                        ),
                    )

                # vin: inputs (only references, spent-flag is updated later)
                for vin in tx["vin"]:
                    if tx["is_coinbase"]:
                        break  # coinbase has no real previous output
                    prev_txid = vin["prev_txid"]
                    vout_index = vin["vout_index"]
                    c.execute(
                        """
                        INSERT OR IGNORE INTO vin
                          (txid, vout_txid, vout_index)
                        VALUES (?, ?, ?)
                        """,
                        (
                            txid,
                            prev_txid,
                            int(vout_index),
                        ),
                    )

            imported_blocks += 1
            if imported_blocks % 1000 == 0:
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                print(
                    f"[pass2] Imported {imported_blocks} main-chain blocks "
                    f"(up to height {b_height})"
                )

            seq_index += 1

        conn.commit()

        if imported_blocks == 0:
            print("[pass2] No main-chain blocks imported.")
            return tip_height

        # Run all post-import steps (confirmations, spent flags, indices, addresses)
        run_postprocessing(conn, tip_height)

        return tip_height

    finally:
        conn.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Import Slimcoin blk*.dat directly into Slimcoin Explorer SQLite DB."
    )
    parser.add_argument(
        "--blocks-dir",
        default=os.path.expanduser("~/.slimcoin"),
        help="Directory containing blkNNNN.dat (default: ~/.slimcoin)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop & recreate explorer tables before import (like a full reindex).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes for header Dcrypt in pass 1 (default: 1). "
             "Pass 2 (block import) always runs single-process to keep memory usage low.",
    )
    parser.add_argument(
        "--post-only",
        action="store_true",
        help="Only run post-processing (confirmations, spent flags, indices, addresses) "
             "on an existing database. No blk*.dat import.",
    )
    args = parser.parse_args()

    blocks_dir = args.blocks_dir
    if not os.path.isdir(blocks_dir):
        raise SystemExit(f"Blocks directory not found: {blocks_dir}")

    if args.reset and args.post_only:
        raise SystemExit("--reset and --post-only cannot be used together.")

    if args.reset:
        print("[init] Reinitializing tables in", DATABASE)
        reinitialize_tables()

    if args.post_only:
        # Only run post-processing on an already imported DB
        print("[init] Post-only mode: running confirmations / spent / indices / addresses...")
        conn = sqlite3.connect(DATABASE, timeout=10)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("PRAGMA busy_timeout=5000")

            cur = conn.cursor()
            cur.execute("SELECT MAX(block_height) FROM blocks")
            row = cur.fetchone()
            tip_height = row[0] if row and row[0] is not None else 0
            print(f"[post-only] Determined tip height {tip_height} from blocks table.")

            run_postprocessing(conn, int(tip_height))
        finally:
            conn.close()
        return

    # Import blocks; return value (tip_height) is not needed here
    import_mainchain_to_db(blocks_dir, workers=args.workers)


if __name__ == "__main__":
    main()