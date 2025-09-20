#!/usr/bin/env python3
"""
split_jsonl_to_parts.py

Split a big JSONL file (one JSON object per line) into `numparts` JSON files (arrays),
with at least one part having exactly `minsize` trades.

Usage:
  python split_jsonl_to_parts.py --infile trades.jsonl --numparts 5 --minsize 100 --outdir parts --seed 42
"""

from __future__ import annotations
import argparse
import os
import sys
import json
import random
from typing import List, Tuple


def read_jsonl_lines(infile: str) -> List[str]:
    """Read non-empty lines from a JSONL file and return as list of raw strings (bytes decoded)."""
    lines: List[str] = []
    with open(infile, "rb") as fh:
        for raw in fh:
            if not raw:
                continue
            s = raw.strip()
            if not s:
                continue
            # keep as text string (utf-8)
            try:
                lines.append(s.decode("utf-8"))
            except UnicodeDecodeError:
                # fallback: decode with errors replaced
                lines.append(s.decode("utf-8", errors="replace"))
    return lines


def compute_part_sizes_exact_one_minsize(total: int, numparts: int, minsize: int, rng: random.Random) -> List[int]:
    """
    Compute sizes for each part ensuring:
      - One randomly chosen part has exactly `minsize`,
      - Remaining total-minsize distributed randomly among the other parts (can be zero).
    Returns list of ints of length numparts summing to total.
    """
    if total < minsize:
        raise ValueError(f"Total records ({total}) < minsize ({minsize})")

    if numparts <= 0:
        raise ValueError("numparts must be >= 1")

    # Special-casing single part
    if numparts == 1:
        # cannot satisfy "one part equals minsize" unless total == minsize
        return [total]

    # choose index for special part
    special_idx = rng.randrange(numparts)

    sizes = [0] * numparts
    sizes[special_idx] = minsize
    remaining = total - minsize

    # Distribute `remaining` among the other (numparts-1) parts randomly.
    # Strategy: generate random non-negative integer partition using random weights:
    #  - if remaining == 0: leave others zero
    if remaining == 0:
        return sizes

    # generate (numparts-1) random weights
    weights = [rng.random() for _ in range(numparts)]
    # set weight of special part to 0 so it gets no more
    weights[special_idx] = 0.0
    # normalize remaining weights
    total_weight = sum(weights)
    if total_weight == 0:
        # all weights zero (rare) -> put everything in a random other part
        target = rng.choice([i for i in range(numparts) if i != special_idx])
        sizes[target] += remaining
        return sizes

    # compute float allocations then floor, track remainder to distribute
    raw_alloc = [ (w / total_weight) * remaining if i != special_idx else 0 for i,w in enumerate(weights) ]
    int_alloc = [int(x) for x in raw_alloc]
    assigned = sum(int_alloc)
    leftover = remaining - assigned

    # distribute leftover 1-by-1 to random parts excluding special_idx, weighted by fractional parts
    frac_parts = [ (raw_alloc[i] - int_alloc[i], i) for i in range(numparts) if i != special_idx ]
    # sort by descending fractional remainder for deterministic-ish allocation, then shuffle equalities
    frac_parts.sort(reverse=True)
    # Now give one by one
    idxs = [i for (_, i) in frac_parts]
    # if leftover > len(idxs), cycle
    j = 0
    while leftover > 0:
        sizes[idxs[j % len(idxs)]] += 1
        j += 1
        leftover -= 1

    # now add integer allocations
    for i in range(numparts):
        if i == special_idx:
            continue
        sizes[i] += int_alloc[i]

    # final sanity
    assert sum(sizes) == total
    # ensure special idx exactly minsize
    assert sizes[special_idx] == minsize

    return sizes


def write_parts(lines: List[str], sizes: List[int], outdir: str, prefix: str = "part_") -> List[str]:
    """Write parts as JSON arrays. Return list of written file paths."""
    os.makedirs(outdir, exist_ok=True)
    written_files: List[str] = []
    idx = 0
    for part_no, sz in enumerate(sizes):
        part_lines = lines[idx: idx + sz]
        idx += sz
        arr = []
        for ln in part_lines:
            # parse line to JSON object to produce pretty/valid JSON arrays
            try:
                obj = json.loads(ln)
            except Exception:
                # fallback: include raw text (shouldn't happen for valid jsonl)
                obj = ln
            arr.append(obj)

        outpath = os.path.join(outdir, f"{prefix}{part_no}.json")
        with open(outpath, "w", encoding="utf-8") as fh:
            # write compact array to save space; change indent if you want pretty
            json.dump(arr, fh, separators=(",", ":"), ensure_ascii=False)
        written_files.append(outpath)
    return written_files


def parse_args():
    p = argparse.ArgumentParser(description="Split a JSONL file into random JSON part files.")
    p.add_argument("--infile", "-i", required=True, help="Input JSONL file (one JSON object per line).")
    p.add_argument("--outdir", "-o", default="parts", help="Output directory for JSON part files.")
    p.add_argument("--numparts", "-n", type=int, required=True, help="Number of part JSON files to create.")
    p.add_argument("--minsize", "-m", type=int, required=True,
                   help="Minimum number of trades for the special part (one part will have exactly this size).")
    p.add_argument("--seed", "-s", type=int, default=None, help="Optional random seed for reproducible splits.")
    return p.parse_args()


def main():
    args = parse_args()
    infile = args.infile
    outdir = args.outdir
    numparts = args.numparts
    minsize = args.minsize
    seed = args.seed

    if not os.path.isfile(infile):
        print(f"ERROR: infile not found: {infile}", file=sys.stderr)
        sys.exit(2)
    if numparts <= 0:
        print("ERROR: numparts must be >= 1", file=sys.stderr)
        sys.exit(2)
    if minsize <= 0:
        print("ERROR: minsize must be >= 1", file=sys.stderr)
        sys.exit(2)

    print(f"[info] loading lines from {infile} ...")
    lines = read_jsonl_lines(infile)
    total = len(lines)
    print(f"[info] total records found: {total:,}")

    if total < minsize:
        print(f"ERROR: total records ({total}) < minsize ({minsize})", file=sys.stderr)
        sys.exit(3)

    rng = random.Random(seed)

    if numparts == 1:
        if total != minsize:
            print(f"[warn] numparts==1 and total ({total}) != minsize ({minsize}); "
                  "will produce one file with all records (cannot create a part exactly minsize).", file=sys.stderr)
        sizes = [total]
    else:
        # shuffle records for randomness
        rng.shuffle(lines)
        sizes = compute_part_sizes_exact_one_minsize(total=total, numparts=numparts, minsize=minsize, rng=rng)

    # If we didn't shuffle because numparts==1, shuffle optionally to still randomize order inside
    if numparts == 1:
        rng.shuffle(lines)

    # write parts
    print(f"[info] writing {numparts} parts to directory: {outdir}")
    written = write_parts(lines, sizes, outdir)
    for pth, sz in zip(written, sizes):
        print(f"  wrote {pth}  (records={sz})")

    print("[done]")


if __name__ == "__main__":
    main()


''' USAGE 
python split_jsonl_to_parts.py --infile testdata/trades.jsonl --numparts 5 --minsize 100 --outdir parts --seed 42

'''