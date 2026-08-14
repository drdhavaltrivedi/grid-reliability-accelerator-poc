"""Pulls a small subset of the Low Carbon London smart meter dataset for
consumption-shape realism (CLAUDE_CODE_BUILD_PROMPT.md, Data Sources #3).

Hard Constraint #2: do NOT ingest the full ~167M row dataset. This script
reads a handful of household "block" CSVs out of the source zip's 168 blocks
(without extracting the rest), keeps a subset of households and, per
household, their first --months months of readings (not a single global
calendar window - households joined the panel at different dates, so a
global window would drop most of them), and writes that subset locally as
CSV - suitable for uploading to a Unity Catalog Volume for
pipelines/10_bronze_ingestion.py to Auto Load once a workspace exists.

Source: https://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households
        (CC-BY licensed; also mirrored on Kaggle as "Smart meters in London")

Usage:
    python scripts/fetch_lcl_sample.py --zip-path <path to Partitioned LCL Data.zip>
        --num-households 300 --months 3 --out data/raw/lcl_sample/lcl_sample.csv
"""
from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

import pandas as pd

# Confirmed by inspecting the zip (2026-08-14): 168 files named
# "Small LCL Data/LCL-June2015v2_<N>.csv", ~1M rows / ~20-30 households each.
COL_ID = "LCLid"
COL_TIME = "DateTime"
COL_KWH = "KWH/hh (per half hour) "  # note: source files have a trailing space


def sorted_blocks(zf: zipfile.ZipFile) -> list[str]:
    return sorted(
        (n for n in zf.namelist() if n.lower().endswith(".csv")),
        key=lambda n: int(n.rsplit("_", 1)[-1].split(".")[0]),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip-path", required=True, help="path to the downloaded 'Partitioned LCL Data.zip'")
    parser.add_argument("--num-households", type=int, default=300)
    parser.add_argument("--months", type=int, default=3)
    parser.add_argument("--max-blocks", type=int, default=10, help="cap on how many block CSVs to read while collecting households")
    parser.add_argument("--out", default="data/raw/lcl_sample/lcl_sample.csv")
    args = parser.parse_args()

    zip_path = Path(args.zip_path)
    collected: list[pd.DataFrame] = []
    seen_households: set[str] = set()

    with zipfile.ZipFile(zip_path) as zf:
        blocks = sorted_blocks(zf)
        for i, member in enumerate(blocks[: args.max_blocks]):
            if len(seen_households) >= args.num_households:
                break
            print(f"[fetch_lcl_sample] reading block {i + 1}/{min(args.max_blocks, len(blocks))}: {member}")
            with zf.open(member) as f:
                block_df = pd.read_csv(f)

            block_df[COL_TIME] = pd.to_datetime(block_df[COL_TIME], errors="coerce")
            block_df = block_df.dropna(subset=[COL_TIME])

            new_households = [h for h in block_df[COL_ID].unique() if h not in seen_households]
            take = new_households[: max(0, args.num_households - len(seen_households))]
            seen_households.update(take)

            block_df = block_df[block_df[COL_ID].isin(take)]
            # per-household window: each household's own first `months` months,
            # not a shared calendar window, so late-joining households aren't dropped.
            block_df["_household_start"] = block_df.groupby(COL_ID)[COL_TIME].transform("min")
            block_df["_cutoff"] = block_df["_household_start"] + pd.DateOffset(months=args.months)
            block_df = block_df[block_df[COL_TIME] < block_df["_cutoff"]]
            collected.append(block_df.drop(columns=["_household_start", "_cutoff"]))

    df = pd.concat(collected, ignore_index=True)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(
        f"[fetch_lcl_sample] wrote {len(df):,} rows, {df[COL_ID].nunique()} households, "
        f"{df[COL_TIME].min().date()} to {df[COL_TIME].max().date()} -> {out_path}"
    )


if __name__ == "__main__":
    main()
