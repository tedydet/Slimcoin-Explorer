# dcrypt_pure.py
#
# Pure Python implementation of the Slimcoin Dcrypt hash
# based on the reference code from the 2014 whitepaper.
#
# All functions work with bytes, not str.

import hashlib

SHA256_LEN = 64  # length of SHA256 hexdigest


def _hash_hexdigest_bytes(data: bytes) -> bytes:
    """
    sha256(data).hexdigest() as ASCII bytes (length 64).
    """
    return hashlib.sha256(data).hexdigest().encode("ascii")


def mix_hashed_nums(hashed_data: bytes) -> bytes:
    """
    Port of mix_hashed_nums(hashedData) from the whitepaper.

    hashed_data: 64-byte ASCII hex (output of _hash_hexdigest_bytes).
    Returns: concatenation of intermediate 64-char hex strings, as bytes.
    """
    if len(hashed_data) != SHA256_LEN:
        raise ValueError("hashed_data must be 64 bytes (ASCII hex)")

    # tmp_list starts as 64 bytes of 0xff
    tmp_list = [b"\xff" for _ in range(SHA256_LEN)]
    ret_list = []

    hashed_end = False
    index = 0

    while not hashed_end:
        # i = int(hashedData[index], 16) + 1
        digit_byte = bytes([hashed_data[index]])  # e.g. b'3'
        step = int(digit_byte, 16) + 1
        index += step

        # If we run past the end, wrap & rehash the hex string itself
        if index >= SHA256_LEN:
            index = index % SHA256_LEN
            hashed_data = _hash_hexdigest_bytes(hashed_data)

        # current hex digit
        tmp_val = hashed_data[index:index + 1]  # 1-byte bytes

        # append this digit to tmp_list
        tmp_list.append(tmp_val)

        # hash tmp_list as a single byte string, then replace tmp_list by
        # the resulting 64-byte hex digest, split into 1-byte elements
        tmp_bytes = b"".join(tmp_list)
        new_hex = _hash_hexdigest_bytes(tmp_bytes)
        tmp_list = [bytes([c]) for c in new_hex]

        # termination condition:
        # index is last position AND last digit equals last digest char
        if index == SHA256_LEN - 1 and tmp_val == tmp_list[SHA256_LEN - 1]:
            hashed_end = True

        # append current tmp_list hash to ret_list
        ret_list.append(b"".join(tmp_list))

    # concatenate all intermediate hashes
    return b"".join(ret_list)


def dcrypt(data: bytes) -> bytes:
    """
    Compute Dcrypt(data) and return it as ASCII hex bytes (length 64).

    This corresponds to the whitepaper:
        hashedData = Hash(data)
        return Hash(mix_hashed_nums(hashedData) + data)
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("dcrypt() expects 'data' as bytes")

    hashed_data = _hash_hexdigest_bytes(data)
    mixed = mix_hashed_nums(hashed_data)
    return _hash_hexdigest_bytes(mixed + data)


def dcrypt_hex(data: bytes) -> str:
    """
    Convenience wrapper: return Dcrypt(data) as hex string.
    """
    return dcrypt(data).decode("ascii")


def dcrypt_pow_hash(header80: bytes) -> bytes:
    """
    Dcrypt PoW hash for a Slimcoin 80-byte block header.

    Returns the 32 raw digest bytes (big-endian SHA256 digest).
    """
    if len(header80) != 80:
        raise ValueError(f"Expected 80-byte header, got {len(header80)} bytes")
    h_hex = dcrypt(header80)           # 64 ASCII hex chars
    return bytes.fromhex(h_hex)        # 32 raw bytes


def dcrypt_pow_hash_hex(header80: bytes) -> str:
    """
    Return Dcrypt PoW hash of header as 64-char hex string.
    """
    return dcrypt(header80).decode("ascii")


def dcrypt_pow_hash_hex_reversed(header80: bytes) -> str:
    """
    Same as above, but bytes reversed (for the usual uint256 / RPC style).
    Use this if the node prints hashes byte-reversed.
    """
    return dcrypt_pow_hash(header80)[::-1].hex()


if __name__ == "__main__":
    # self-test using the whitepaper vectors
    s1 = b"The quick brown fox jumps over the lazy dog"
    s2 = b"The quick brown fox jumps over the lazy dog."

    print("test1:", dcrypt_hex(s1))
    print("test2:", dcrypt_hex(s2))
    # expected (from whitepaper)
    print("exp1 : a74369ea2f6434aa55e38820d35300ba6130d82e0121ef64de6218c9198a377d")
    print("exp2 : 559c1114f4e9de3bb12e0d1681258503c929f84527936b9d83f7c4d32a4fa67c")