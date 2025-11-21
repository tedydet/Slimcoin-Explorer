#!/usr/bin/env python3
"""
Slimcoin blk*.dat -> blockchain.db importer

- Uses the same DB schema as database.py (blocks, transactions, vin, vout, addresses)
- Uses reinitialize_tables(), drop_indices(), create_indices(), rebuild_addresses()
  from database.py
- Reads Slimcoin raw blocks directly from blkNNNN.dat files
- No RPC, no JSON blocks
"""

import os
import sys
import struct
import sqlite3
import hashlib
from io import BytesIO

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
    version. The txid is always computed from the canonical Bitcoin-style
    layout (version, vin, vout, locktime) without the nTime field, to match
    Slimcoin's/Peercoin's txid semantics.
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

    # Reconstruct canonical bytes for txid hashing (without nTime)
    from io import BytesIO as _BytesIO
    buf = _BytesIO()
    buf.write(struct.pack("<I", v))
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
    Parse the 80-byte header and compute block hash / prev-hash according
    to the chosen endianness model.
    """
    if len(header) < 80:
        raise ValueError("Block header too short")

    version = struct.unpack("<I", header[:4])[0]
    raw_prev = header[4:36]
    raw_merkle = header[36:68]
    timestamp, bits, nonce = struct.unpack("<III", header[68:80])

    if reversed_hash:
        block_hash = hash256(header)[::-1].hex()
        prev_block = raw_prev[::-1].hex()
        merkle_root = raw_merkle[::-1].hex()
    else:
        block_hash = hash256(header).hex()
        prev_block = raw_prev.hex()
        merkle_root = raw_merkle.hex()

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
    Parse the 80-byte header and compute block hash, using the globally
    selected endianness model (USE_REVERSED_HASH).
    """
    header = block_bytes[:80]
    return parse_block_header_bytes(header, USE_REVERSED_HASH)



# --- robust transaction parser for Slimcoin blocks (handles PoB extra data) ---
def parse_block_transactions(block_bytes: bytes):
    """Parse all transactions from a Slimcoin block body.

    Some Slimcoin blocks (e.g. proof-of-burn or other special cases) can carry
    extra data before or after the transaction list. We therefore:
      - try multiple candidate offsets ("skip" bytes) between the 80-byte
        header and the tx-count varint,
      - try both tx layouts (with and without nTime),
      - choose the variant that leaves the *smallest* unused tail in the
        block body.
    """
    if len(block_bytes) <= 80:
        return []

    body = block_bytes[80:]
    last_error = None

    best_txs = None
    best_remaining = None

    # Try candidate offsets up to 256 bytes; normal blocks succeed at skip=0.
    # Some Slimcoin blocks (especially PoS/PoB) appear to have a longer
    # extended header between the 80-byte core header and the tx vector.
    max_skip = min(256, len(body))
    for skip in range(0, max_skip + 1):
        for allow_time in (True, False):
            f = BytesIO(body[skip:])
            try:
                tx_count = read_varint(f)
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

                # Prefer variants that leave less unparsed data at the end.
                if best_txs is None or remaining < best_remaining:
                    best_txs = txs
                    best_remaining = remaining

                    # If we have a perfect match (no remaining bytes), we can
                    # stop searching.
                    if best_remaining == 0:
                        return best_txs

            except Exception as e:
                last_error = e
                continue

    if best_txs is not None:
        # We found at least one variant that parsed tx_count transactions
        # without overrunning the buffer; accept the best match even if some
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

    txs = parse_block_transactions(block_bytes)

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


# ----------- main-chain discovery (pass 1) -----------


def _build_chain_for_headers(headers):
    """
    Given a headers dict of the form
      {block_hash: {"prev": ..., "time": ..., "bits": ..., "size": ...}}
    compute:
      - height_map
      - main_chain (set of block hashes)
      - tip_hash, tip_height
      - diff1_target from the (first) genesis bits
    """
    height_map = {}
    diff1_target = None

    def compute_height(bh):
        nonlocal diff1_target
        if bh in height_map:
            return height_map[bh]
        prev = headers[bh]["prev"]
        if not prev or prev not in headers:
            # genesis
            height_map[bh] = 0
            bits = headers[bh]["bits"]
            exponent = bits >> 24
            mantissa = bits & 0xFFFFFF
            diff1_target_local = mantissa * (1 << (8 * (exponent - 3)))
            if diff1_target is None:
                diff1_target = diff1_target_local
            return 0
        h_prev = compute_height(prev)
        height_map[bh] = h_prev + 1
        return height_map[bh]

    for bh in list(headers.keys()):
        compute_height(bh)

    if not height_map:
        raise RuntimeError("No block heights computed (empty headers?)")

    tip_hash = max(height_map.keys(), key=lambda bh: height_map[bh])
    tip_height = height_map[tip_hash]

    # walk back from tip to genesis to get main chain set
    main_chain = set()
    cur = tip_hash
    while True:
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


def build_chain_headers(blocks_dir):
    """
    Pass 1:
      - scan all headers from blk*.dat
      - try both possible hash/prev endianness conventions
      - pick the one that yields the highest tip height
      - compute heights and determine main chain
    Returns:
      headers:    {block_hash: {prev, time, bits, size}}
      height_map: {block_hash: height}
      main_chain: set(block_hash) for main chain only
      tip_hash, tip_height
      diff1_target: target for difficulty=1 derived from genesis bits
    """
    global USE_REVERSED_HASH

    headers_A = {}  # reversed hashes (Bitcoin-style)
    headers_B = {}  # raw little-endian hashes

    print("[pass1] Scanning blk*.dat headers...")
    for block_bytes, block_size in iter_block_bytes(blocks_dir):
        if len(block_bytes) < 80:
            continue
        header = block_bytes[:80]

        # Variant A: reversed hashes (usual Bitcoin / RPC-style)
        hA = parse_block_header_bytes(header, True)
        headers_A[hA["hash"]] = {
            "prev": hA["prev_block"],
            "time": hA["time"],
            "bits": hA["bits"],
            "size": block_size,
        }

        # Variant B: raw little-endian hex strings
        hB = parse_block_header_bytes(header, False)
        headers_B[hB["hash"]] = {
            "prev": hB["prev_block"],
            "time": hB["time"],
            "bits": hB["bits"],
            "size": block_size,
        }

    if not headers_A or not headers_B:
        raise RuntimeError("No blocks found in blk*.dat")

    # Build chains for both variants
    hmap_A, main_A, tip_A_hash, tip_A_height, diff1_A = _build_chain_for_headers(headers_A)
    hmap_B, main_B, tip_B_hash, tip_B_height, diff1_B = _build_chain_for_headers(headers_B)

    print(f"[pass1] Variant A (reversed hashes): tip height {tip_A_height}, tip hash {tip_A_hash}")
    print(f"[pass1] Variant B (raw little-endian): tip height {tip_B_height}, tip hash {tip_B_hash}")

    # Choose the variant with the higher tip height
    if tip_A_height >= tip_B_height:
        USE_REVERSED_HASH = True
        headers = headers_A
        height_map = hmap_A
        main_chain = main_A
        tip_hash = tip_A_hash
        tip_height = tip_A_height
        diff1_target = diff1_A
        chosen = "A (reversed hashes / Bitcoin-style)"
    else:
        USE_REVERSED_HASH = False
        headers = headers_B
        height_map = hmap_B
        main_chain = main_B
        tip_hash = tip_B_hash
        tip_height = tip_B_height
        diff1_target = diff1_B
        chosen = "B (raw little-endian hashes)"

    print(f"[pass1] Using variant {chosen}")
    print(f"[pass1] Found {len(headers)} blocks in files.")

    return headers, height_map, main_chain, tip_hash, tip_height, diff1_target


# ----------- import into your DB (pass 2) -----------


def import_mainchain_to_db(blocks_dir):
    """
    Single-pass import:
      - stream all blocks in file order
      - parse full blocks
      - assign block_height sequentially starting at 0
      - write directly into Explorer DB
    This deliberately ignores possible side-chain/orphan blocks and treats
    the blkNNNN.dat sequence as canonical, which is sufficient for the
    explorer use case.
    """
    print("[pass2] Importing all blocks sequentially into SQLite...")

    conn = sqlite3.connect(DATABASE, timeout=10)
    try:
        # similar performance tuning as in reindex_db
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-20000")
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("PRAGMA busy_timeout=5000")

        drop_indices(conn)
        c = conn.cursor()

        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")

        imported_blocks = 0
        current_height = 0
        last_height = -1
        diff1_target = None

        for block_bytes, block_size in iter_block_bytes(blocks_dir):
            try:
                block = parse_full_block(block_bytes)
            except Exception as e:
                # Try to decode at least the header so we know which block failed
                try:
                    hdr = parse_block_header(block_bytes)
                    bh_fail = hdr["hash"]
                    btime_fail = hdr["time"]
                    bits_fail = hdr["bits"]
                    print(
                        f"[warn] Failed to parse block at sequential height {current_height}, "
                        f"hash {bh_fail}, time {btime_fail}, bits {bits_fail}: {e}"
                    )
                except Exception as e2:
                    print(
                        f"[warn] Failed to parse block at sequential height {current_height}, "
                        f"and header parsing also failed: {e} / header error: {e2}"
                    )
                continue

            bh = block["hash"]
            b_height = current_height
            current_height += 1
            last_height = b_height

            b_time = block["time"]
            bits = block["bits"]

            # initialize diff1_target from the first successfully parsed block
            if diff1_target is None:
                exponent = bits >> 24
                mantissa = bits & 0xFFFFFF
                diff1_target = mantissa * (1 << (8 * (exponent - 3)))

            difficulty = difficulty_from_bits(bits, diff1_target)

            # insert block row
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

            # insert transactions
            for tx in block["txs"]:
                txid = tx["txid"]
                is_coinbase = tx["is_coinbase"]

                # sum of outputs in SLM units
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
                        INSERT INTO vout
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
                        INSERT INTO vin
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
                print(f"[pass2] Imported {imported_blocks} blocks (up to height {b_height})")

        conn.commit()

        tip_height = last_height if last_height >= 0 else 0

        if imported_blocks == 0:
            print("[pass2] No blocks imported.")
            return tip_height

        # set confirmations in bulk
        print("[post] Updating confirmations...")
        c.execute(
            "UPDATE blocks SET confirmations = ? - block_height + 1",
            (int(tip_height),),
        )
        conn.commit()

        # rebuild spent flags from vin
        print("[post] Rebuilding spent flags from vin...")
        c.execute("UPDATE vout SET spent = 0")
        c.execute(
            """
            UPDATE vout
            SET spent = 1
            WHERE EXISTS (
                SELECT 1 FROM vin
                WHERE vin.vout_txid = vout.txid
                  AND vin.vout_index = vout.ind
            )
            """
        )
        conn.commit()

        # recreate indices
        print("[post] Creating indices...")
        create_indices(conn)
        conn.commit()

        # rebuild addresses table from vout
        print("[post] Rebuilding addresses table...")
        rebuild_addresses(conn, int(tip_height))
        print("[done] Import finished.")

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
        default=os.path.expanduser("~/.slimcoin/blocks"),
        help="Directory containing blkNNNN.dat (default: ~/.slimcoin/blocks)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop & recreate explorer tables before import (like a full reindex).",
    )
    args = parser.parse_args()

    blocks_dir = args.blocks_dir
    if not os.path.isdir(blocks_dir):
        raise SystemExit(f"Blocks directory not found: {blocks_dir}")

    if args.reset:
        print("[init] Reinitializing tables in", DATABASE)
        reinitialize_tables()

    # Import blocks sequentially; return value (tip_height) is not needed here
    import_mainchain_to_db(blocks_dir)


if __name__ == "__main__":
    main()