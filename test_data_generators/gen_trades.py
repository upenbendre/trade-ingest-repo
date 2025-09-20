#!/usr/bin/env python3
"""
gen_trades.py

Generate random trade JSON records using polars (Rust-backed) for performance.

Example:
  python gen_trades.py --num 10000 --v1 30 --vn 40 --out trades.jsonl

This will create 10,000 records where ~30% are version 1 unique trades and ~40% are version 2-4 records
referencing trade_ids from the v1 set.

"""

#pip install orjson polars

import argparse
import random
import datetime
import math
import json
import sys

try:
    import polars as pl
except Exception as e:
    raise RuntimeError(
        "polars is required. Install with: pip install polars\n"
        "Polars is a fast Rust-backed dataframe library."
    ) from e

# try to use orjson for faster JSON serialization if available
try:
    import orjson as _orjson

    def dumps_json(obj, pretty=False):
        if pretty:
            return _orjson.dumps(obj, option=_orjson.OPT_INDENT_2).decode()
        return _orjson.dumps(obj).decode()

except Exception:
    def dumps_json(obj, pretty=False):
        return json.dumps(obj, indent=2 if pretty else None, default=str)


def format_ddmmyyyy(d: datetime.date) -> str:
    return d.strftime("%d/%m/%Y")


def random_date_between(start: datetime.date, end: datetime.date) -> datetime.date:
    days = (end - start).days
    if days <= 0:
        return start
    return start + datetime.timedelta(days=random.randint(0, days))


def build_trades(num_of_recs: int, v1_perc: float, vn_perc: float, json_format: str = "jsonl"):
    """
    Returns list of dicts (records).
    v1_perc and vn_perc are percentages (0-100).
    json_format: 'jsonl' (default) or 'array'
    """
    today = datetime.date.today()

    # compute counts
    num_v1 = int(round(num_of_recs * (v1_perc / 100.0)))
    num_vn = int(round(num_of_recs * (vn_perc / 100.0)))
    # remaining records are "other" trades (random)
    num_remaining = num_of_recs - num_v1 - num_vn
    if num_remaining < 0:
        # scale down vn if too big
        excess = -num_remaining
        if num_vn >= excess:
            num_vn -= excess
            num_remaining = 0
        else:
            # resort to clipping
            num_vn = 0
            num_v1 = max(0, num_of_recs)
            num_remaining = 0

    # generate v1 unique trade ids: T0001, T0002 ...
    v1_trade_ids = [f"T{str(i+1).zfill(5)}" for i in range(num_v1)]

    records = []

    # helper to produce synthetic counterparty and book ids
    cp_list = [f"CP-{i}" for i in range(1, 101)]
    book_list = [f"B{i}" for i in range(1, 51)]

    # generate version 1 records (unique trade ids)
    for tid in v1_trade_ids:
        version = 1
        cp = random.choice(cp_list)
        book = random.choice(book_list)
        # maturity between today-5yrs and today+5yrs
        maturity = random_date_between(today - datetime.timedelta(days=5*365),
                                       today + datetime.timedelta(days=5*365))
        # created_date: some are today, some older (random within last 10 years)
        if random.random() < 0.6:
            created = today
        else:
            created = random_date_between(today - datetime.timedelta(days=10*365), today)
        expired = "Y" if maturity < today else "N"
        records.append({
            "Trade Id": tid,
            "Version": version,
            "Counter-Party_Id": cp,
            "Book-Id": book,
            "Maturity_Date": format_ddmmyyyy(maturity),
            "Created_Date": format_ddmmyyyy(created),
            "Expired": expired
        })

    # generate vn records referencing v1 trade ids (versions 2..4)
    # distribute versions for records referencing same trade id, allow multiple per trade id
    if num_v1 == 0 and num_vn > 0:
        # no v1 to reference; fallback by creating new v1 ids
        v1_trade_ids = [f"T{str(i+1).zfill(5)}" for i in range(max(1, num_vn))]
        num_v1 = len(v1_trade_ids)
    for _ in range(num_vn):
        tid = random.choice(v1_trade_ids)
        version = random.randint(2, 4)
        cp = random.choice(cp_list)
        book = random.choice(book_list)
        maturity = random_date_between(today - datetime.timedelta(days=5*365),
                                       today + datetime.timedelta(days=5*365))
        # created_date: more likely to be today for higher versions (simulate new updates)
        if random.random() < 0.8:
            created = today
        else:
            created = random_date_between(today - datetime.timedelta(days=10*365), today)
        expired = "Y" if maturity < today else "N"
        records.append({
            "Trade Id": tid,
            "Version": version,
            "Counter-Party_Id": cp,
            "Book-Id": book,
            "Maturity_Date": format_ddmmyyyy(maturity),
            "Created_Date": format_ddmmyyyy(created),
            "Expired": expired
        })

    # remaining records: random new trade ids and random versions (1..4)
    current_count = num_v1
    for i in range(num_remaining):
        # continue numbering
        current_count += 1
        tid = f"T{str(current_count + 100000).zfill(6)}"  # distinct range to avoid colliding with v1 ids
        version = random.randint(1, 4)
        cp = random.choice(cp_list)
        book = random.choice(book_list)
        maturity = random_date_between(today - datetime.timedelta(days=5*365),
                                       today + datetime.timedelta(days=5*365))
        created = random.choice([today, random_date_between(today - datetime.timedelta(days=10*365), today)])
        expired = "Y" if maturity < today else "N"
        records.append({
            "Trade Id": tid,
            "Version": version,
            "Counter-Party_Id": cp,
            "Book-Id": book,
            "Maturity_Date": format_ddmmyyyy(maturity),
            "Created_Date": format_ddmmyyyy(created),
            "Expired": expired
        })

    # shuffle to avoid ordering artifacts
    random.shuffle(records)

    # Build polars DataFrame for speed and potential downstream operations
    df = pl.DataFrame(records)

    # ensure types (Version as int)
    df = df.with_columns(pl.col("Version").cast(pl.Int64))

    # final record count check
    assert df.height == num_of_recs, f"expected {num_of_recs} records but produced {df.height}"

    # convert to list of dicts for JSON output
    recs = df.to_dicts()
    return recs


def main():
    p = argparse.ArgumentParser(description="Generate random trade JSON records.")
    p.add_argument("--num", "--num_of_recs", dest="num", type=int, required=True,
                   help="Total number of JSON records to generate.")
    p.add_argument("--v1", dest="v1", type=float, default=20.0,
                   help="Percentage of version 1 records (unique trade ids).")
    p.add_argument("--vn", dest="vn", type=float, default=30.0,
                   help="Percentage of version 2-4 records that reference v1 trade ids.")
    p.add_argument("--out", dest="out", type=str, default="-",
                   help="Output filename (default stdout). Use .jsonl for JSON lines, .json for array.")
    p.add_argument("--format", dest="format", choices=["jsonl", "array"], default=None,
                   help="Output format. If not given, chosen from filename: .jsonl -> jsonl, .json -> array.")
    p.add_argument("--pretty", dest="pretty", action="store_true",
                   help="Pretty-print JSON (uses more space).")

    args = p.parse_args()

    if args.num <= 0:
        print("num must be > 0", file=sys.stderr)
        sys.exit(2)
    if not (0 <= args.v1 <= 100 and 0 <= args.vn <= 100):
        print("v1 and vn must be percentages between 0 and 100", file=sys.stderr)
        sys.exit(2)

    # decide format
    fmt = args.format
    if fmt is None:
        if args.out.endswith(".jsonl") or args.out == "-":
            fmt = "jsonl"
        elif args.out.endswith(".json"):
            fmt = "array"
        else:
            fmt = "jsonl"

    recs = build_trades(args.num, args.v1, args.vn, json_format=fmt)

    # write out
    if args.out == "-" or args.out == "":
        # stdout
        if fmt == "jsonl":
            for r in recs:
                print(dumps_json(r, pretty=args.pretty))
        else:
            print(dumps_json(recs, pretty=args.pretty))
    else:
        mode = "w"
        with open(args.out, mode, encoding="utf-8") as f:
            if fmt == "jsonl":
                for r in recs:
                    f.write(dumps_json(r) + "\n")
            else:
                f.write(dumps_json(recs, pretty=args.pretty))
        print(f"Wrote {len(recs)} records to {args.out}")


if __name__ == "__main__":
    main()
