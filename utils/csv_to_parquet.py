#!/usr/bin/env python3
"""
csv_to_parquet.py

Convert one or many CSV files to Parquet format.

Usage:
    python csv_to_parquet.py --src <source_path> --dest <destination_path>

Examples:
    # Convert a single CSV to a Parquet file
    python csv_to_parquet.py --src data/raw/dolt/options_quotes.csv --dest data/processed/options_quotes.parquet

    # Convert all CSV files in a folder to Parquet (outputs to same folder)
    python csv_to_parquet.py --src data/raw/dolt/ --dest data/processed/
"""

import os
import sys
import argparse
import pandas as pd
from pathlib import Path

def convert_csv_to_parquet(src: str, dest: str):
    src_path = Path(src)
    dest_path = Path(dest)

    # If source is a single file
    if src_path.is_file() and src_path.suffix.lower() == ".csv":
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if dest_path.is_dir():
            dest_file = dest_path / (src_path.stem + ".parquet")
        else:
            dest_file = dest_path
        print(f"Converting {src_path} -> {dest_file}")
        df = pd.read_csv(src_path)
        df.to_parquet(dest_file, engine="pyarrow", index=False)
        return

    # If source is a directory (batch convert all CSVs)
    if src_path.is_dir():
        dest_path.mkdir(parents=True, exist_ok=True)
        for csv_file in src_path.glob("*.csv"):
            dest_file = dest_path / (csv_file.stem + ".parquet")
            print(f"Converting {csv_file} -> {dest_file}")
            df = pd.read_csv(csv_file)
            df.to_parquet(dest_file, engine="pyarrow", index=False)
        return

    print(f"Error: {src} is not a CSV file or directory.")
    sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet format.")
    parser.add_argument("--src", required=True, help="Source CSV file or directory")
    parser.add_argument("--dest", required=True, help="Destination Parquet file or directory")

    args = parser.parse_args()
    convert_csv_to_parquet(args.src, args.dest)
