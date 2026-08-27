#!/usr/bin/env python3
"""Generate a leveldb store whose table file holds keys in the wrong order.

This is the corruption seen in the field (Nexus-Mods/Vortex#23981): a table
whose entries are not sorted. leveldb only asserts sortedness in
TableBuilder::Add under a debug build, so a release build writes such a table
silently, and every later compaction that reads it inherits the disorder.
Reads then skip whole runs, because both the merging iterator and the DB
iterator assume each input is sorted.

The tables are written here by hand rather than through leveldb, so the
fixture can still be regenerated once TableBuilder rejects unsorted input.

Usage: make_unsorted_fixture.py <output-dir>
"""
import os
import struct
import sys

# crc32c (Castagnoli), reflected polynomial
_CRC_TABLE = []
for _i in range(256):
    _c = _i
    for _ in range(8):
        _c = (_c >> 1) ^ (0x82F63B78 if _c & 1 else 0)
    _CRC_TABLE.append(_c)


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc = (crc >> 8) ^ _CRC_TABLE[(crc ^ byte) & 0xFF]
    return crc ^ 0xFFFFFFFF


def mask_crc(crc: int) -> int:
    """leveldb stores CRCs masked so they survive being embedded in a record."""
    return (((crc >> 15) | (crc << 17)) + 0xA282EAD8) & 0xFFFFFFFF


def varint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


TYPE_DELETION = 0
TYPE_VALUE = 1


def internal_key(user_key: str, seq: int, key_type: int = TYPE_VALUE) -> bytes:
    """user key plus the 8-byte trailer of (sequence << 8 | type)."""
    return user_key.encode("utf-8") + struct.pack("<Q", (seq << 8) | key_type)


def build_block(entries):
    """A data/index block: entries with no prefix compression, one restart."""
    body = bytearray()
    restarts = []
    for key, value in entries:
        restarts.append(len(body))
        body += varint(0)  # shared bytes
        body += varint(len(key))
        body += varint(len(value))
        body += key
        body += value
    for offset in restarts:
        body += struct.pack("<I", offset)
    body += struct.pack("<I", len(restarts))
    return bytes(body)


def append_block(out: bytearray, block: bytes) -> tuple:
    """Append a block plus its trailer; returns its (offset, size) handle."""
    offset = len(out)
    out += block
    out += bytes([0])  # no compression
    out += struct.pack("<I", mask_crc(crc32c(block + bytes([0]))))
    return offset, len(block)


def write_table(path: str, blocks_of_entries):
    """Write an sstable whose data blocks appear in the given order."""
    out = bytearray()
    index_entries = []
    for entries in blocks_of_entries:
        offset, size = append_block(out, build_block(entries))
        # the index separator is the block's own last key, which is what
        # leveldb records when the input is ordered
        separator = entries[-1][0]
        index_entries.append((separator, varint(offset) + varint(size)))

    meta_offset, meta_size = append_block(out, build_block([]))
    index_offset, index_size = append_block(out, build_block(index_entries))

    footer = bytearray()
    footer += varint(meta_offset) + varint(meta_size)
    footer += varint(index_offset) + varint(index_size)
    footer += bytes(40 - len(footer))
    footer += struct.pack("<Q", 0xDB4775248B80FB57)
    out += footer

    with open(path, "wb") as handle:
        handle.write(bytes(out))
    return len(out)


def log_record(payload: bytes) -> bytes:
    """One FULL log record: masked crc32c, length, type, payload."""
    header = struct.pack("<I", mask_crc(crc32c(bytes([1]) + payload)))
    header += struct.pack("<H", len(payload))
    header += bytes([1])  # kFullType
    return header + payload


def version_edit(files, last_sequence: int) -> bytes:
    """A VersionEdit naming the given files, with the bookkeeping leveldb needs."""
    out = bytearray()
    out += varint(1) + varint(len("leveldb.BytewiseComparator"))
    out += b"leveldb.BytewiseComparator"
    out += varint(2) + varint(0)  # log number
    out += varint(3) + varint(20)  # next file number
    out += varint(4) + varint(last_sequence)
    for level, number, size, smallest, largest in files:
        out += varint(7)  # kNewFile
        out += varint(level) + varint(number) + varint(size)
        out += varint(len(smallest)) + smallest
        out += varint(len(largest)) + largest
    return bytes(out)


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    out_dir = sys.argv[1]
    # Start from an empty directory: leftovers from a previous fixture, or
    # from anything that opened it, would be picked up as part of the store.
    if os.path.isdir(out_dir):
        for name in os.listdir(out_dir):
            path = os.path.join(out_dir, name)
            if os.path.isfile(path):
                os.remove(path)
    os.makedirs(out_dir, exist_ok=True)

    # Two blocks whose contents are individually sorted but which appear in the
    # wrong order relative to each other, plus one pair swapped inside the
    # second block - both shapes were present in the field stores.
    seq = 100
    block_high = [(internal_key("key_c_%02d" % i, seq + i), b"high-%02d" % i) for i in range(8)]
    block_low = [(internal_key("key_a_%02d" % i, seq + 20 + i), b"low-%02d" % i) for i in range(8)]
    block_low[3], block_low[4] = block_low[4], block_low[3]

    table_path = os.path.join(out_dir, "000005.ldb")
    size = write_table(table_path, [block_high, block_low])

    # a second, healthy table so tests can tell a targeted report from a
    # blanket one
    block_ok = [(internal_key("key_z_%02d" % i, seq + 40 + i), b"ok-%02d" % i) for i in range(8)]
    ok_path = os.path.join(out_dir, "000006.ldb")
    ok_size = write_table(ok_path, [block_ok])

    # A table the descriptor does not reference, holding a key that was later
    # deleted. leveldb drops a tombstone once the key reaches the base level,
    # so the deletion leaves no trace beyond the key's absence from the live
    # tables - and anything that rebuilds a store by reading whatever .ldb
    # files it finds will bring this key back from the dead.
    block_obsolete = [
        (internal_key("key_deleted_%02d" % i, seq + 60 + i), b"resurrected-%02d" % i)
        for i in range(4)
    ]
    obsolete_path = os.path.join(out_dir, "000007.ldb")
    write_table(obsolete_path, [block_obsolete])

    # The descriptor records each table's first and last key, exactly as
    # leveldb derives them, so the ranges inherit the disorder. 000007 is
    # deliberately absent: it is obsolete, and leveldb would ignore it.
    files = [
        (0, 5, size, block_high[0][0], block_low[-1][0]),
        (0, 6, ok_size, block_ok[0][0], block_ok[-1][0]),
    ]
    manifest = os.path.join(out_dir, "MANIFEST-000001")
    with open(manifest, "wb") as handle:
        handle.write(log_record(version_edit(files, seq + 60)))
    # binary mode: leveldb strips exactly one trailing "\n", so a CRLF here
    # would leave a stray CR in the descriptor's file name
    with open(os.path.join(out_dir, "CURRENT"), "wb") as handle:
        handle.write(b"MANIFEST-000001\n")

    print("wrote fixture to %s" % out_dir)
    print("  000005.ldb: blocks out of order, and one swapped pair within a block")
    print("  000006.ldb: sorted")
    print("  000007.ldb: sorted, but obsolete - not referenced by the descriptor")
    return 0


if __name__ == "__main__":
    sys.exit(main())
