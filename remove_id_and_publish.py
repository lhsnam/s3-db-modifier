#!/usr/bin/env python3
"""
remove_id_and_publish.py

Usage:
  python remove_id_and_publish.py \
    -i GCA_013364925.1,GCA_005768625.2,... \
    [--exclude-db virus,bact] \
    [--src-prefix sourmash-databases/k21/] \
    [--dst-prefix sourmash-databases-2506/k21/] \
    [--bucket io-pipeline-references] \
    [--region <region>] \
    [--workdir /path/to/workdir] \
    -h | --help

What it does:
  • Scans all objects under src-prefix in the given S3 bucket.
  • Optionally skips any “database” (subfolder) whose name contains an
    exclude substring.
  • For each remaining CSV: downloads it, removes any row where “name”
    matches an input ID, and re-uploads the cleaned CSV.
  • For each remaining ZIP: downloads, extracts, removes signatures for
    matching IDs (via SOURMASH-MANIFEST.csv), rebuilds with a progress
    bar, and re-uploads.
  • Shows one big progress bar for every sub-step (download, process,
    rebuild, upload). Smaller dim bars show byte‐level progress for each file.
  • At the end, prints a table: rows = IDs, columns = databases (subfolders
    under k21/k31/k51), with “✓” centered in blue if cleaned there, or “✗”
    centered in red if not.

Safety:
  • dst-prefix must not be “sourmash-databases/” to avoid overwriting the
    original.
  • src-prefix and dst-prefix cannot be the same or nested.
"""

import argparse
import boto3
import botocore
import csv
import os
import sys
import tempfile
import zipfile
import threading
from tqdm import tqdm

class ProgressPercentage:
    """
    Callback for boto3 to show a dim, byte‐level tqdm bar for downloads/uploads.
    """
    def __init__(self, filename, size):
        self._filename = filename
        self._size = size
        self._seen = 0
        self._lock = threading.Lock()
        self._pbar = tqdm(
            total=size,
            unit="B",
            unit_scale=True,
            desc=f"\033[2m{filename}\033[0m",
            leave=False,
            position=1,
            bar_format='\033[2m{l_bar}{bar}{r_bar}\033[0m'
        )

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen += bytes_amount
            self._pbar.update(bytes_amount)
            if self._seen >= self._size:
                self._pbar.close()

def parse_args():
    """
    Parse command-line arguments. -h/--help is automatic.
    """
    parser = argparse.ArgumentParser(
        description="Remove matching IDs from CSVs and ZIPs, skipping specified databases."
    )
    parser.add_argument(
        "-i", "--ids", required=True,
        help="Comma-separated list of IDs to remove (e.g. GCA_013364925.1,GCA_005768625.2)."
    )
    parser.add_argument(
        "-e", "--exclude-db", default="",
        help="Comma-separated list of substrings. Databases whose names contain these substrings will be skipped."
    )
    parser.add_argument(
        "-b", "--bucket", default="io-pipeline-references",
        help="S3 bucket name (default: io-pipeline-references)."
    )
    parser.add_argument(
        "-s", "--src-prefix", default="sourmash-databases/k21/",
        help="Source S3 prefix (default: sourmash-databases/k21/)."
    )
    parser.add_argument(
        "-d", "--dst-prefix", default="sourmash-databases-2506/k21/",
        help="Destination S3 prefix (default: sourmash-databases-2506/k21/)."
    )
    parser.add_argument(
        "-r", "--region", default=None,
        help="AWS region (optional)."
    )
    parser.add_argument(
        "-w", "--workdir", default=None,
        help="Local directory for extracting/rebuilding ZIPs. Defaults to ./WORK."
    )
    return parser.parse_args()

def list_all_keys(s3_client, bucket, prefix):
    """
    Yield every S3 key under the given prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def main():
    args = parse_args()

    # Build ID set
    target_ids = {i.strip() for i in args.ids.split(",") if i.strip()}
    id_status = {tid: False for tid in target_ids}

    # Build exclude‐db substrings
    exclude_subs = [s.strip() for s in args.exclude_db.split(",") if s.strip()]

    bucket = args.bucket
    src_prefix = args.src_prefix.rstrip("/") + "/"
    dst_prefix = args.dst_prefix.rstrip("/") + "/"

    # Prevent dst-prefix = "sourmash-databases/"
    if dst_prefix.rstrip("/") == "sourmash-databases":
        print("Error: dst-prefix cannot be 'sourmash-databases/'. Aborting.")
        sys.exit(1)

    # Prevent src/dst same or nested
    if src_prefix == dst_prefix or dst_prefix.startswith(src_prefix) or src_prefix.startswith(dst_prefix):
        print(f"Error: src-prefix ('{src_prefix}') and dst-prefix ('{dst_prefix}') "
              "must not be the same or nested. Aborting.")
        sys.exit(1)

    # Base workdir
    if args.workdir:
        base_workdir = args.workdir
    else:
        base_workdir = os.path.join(os.getcwd(), "WORK")
    os.makedirs(base_workdir, exist_ok=True)

    # Create S3 client
    s3_kwargs = {}
    if args.region:
        s3_kwargs["region_name"] = args.region
    s3_client = boto3.client("s3", **s3_kwargs)

    # 1) List all keys under src_prefix
    all_keys = list(list_all_keys(s3_client, bucket, src_prefix))
    if not all_keys:
        print("No objects under that prefix. Exiting.")
        return

    # Partition keys by database, apply exclude
    keys_by_db = {}
    for key in all_keys:
        if key.endswith("/"):
            continue
        rel = key[len(src_prefix):]
        parts = rel.split("/", 1)
        db = parts[0] if len(parts) > 1 else "__root__"
        # Skip if db contains any exclude substring
        if any(sub in db for sub in exclude_subs):
            continue
        keys_by_db.setdefault(db, []).append(key)

    # Flatten filtered keys
    filtered_keys = [k for db in keys_by_db for k in keys_by_db[db]]
    if not filtered_keys:
        print("After excluding, no keys remain. Exiting.")
        return

    # 2) Count CSVs and ZIPs among filtered keys
    num_csv = sum(1 for k in filtered_keys if k.lower().endswith(".csv"))
    num_zip = sum(1 for k in filtered_keys if k.lower().endswith(".zip"))
    total_steps = num_csv * 3 + num_zip * 5  # CSV=3, ZIP=5

    # Track detection per database
    detection_map = {tid: set() for tid in target_ids}
    databases = sorted(keys_by_db.keys())

    # 3) Main progress bar
    pbar = tqdm(
        total=total_steps,
        desc="\033[1mProcessing\033[0m",
        unit="step",
        leave=False,
        bar_format='\033[1m{l_bar}{bar}{r_bar}\033[0m'
    )

    for db in databases:
        for key in keys_by_db[db]:
            filename = os.path.basename(key)

            if key.lower().endswith(".csv"):
                # CSV: 3 steps
                # Step 1: Download
                pbar.set_description(f"\033[1mDownloading CSV {filename}\033[0m")
                try:
                    head = s3_client.head_object(Bucket=bucket, Key=key)
                    size = head.get("ContentLength", 0)
                except botocore.exceptions.ClientError:
                    size = 0

                with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tf:
                    local_path = tf.name

                if size > 0:
                    cb = ProgressPercentage(filename, size)
                    try:
                        s3_client.download_file(bucket, key, local_path, Callback=cb)
                    except botocore.exceptions.ClientError:
                        os.remove(local_path)
                        pbar.update(1)
                        continue
                else:
                    try:
                        s3_client.download_file(bucket, key, local_path)
                    except botocore.exceptions.ClientError:
                        os.remove(local_path)
                        pbar.update(1)
                        continue
                pbar.update(1)

                # Step 2: Process
                pbar.set_description(f"\033[1mProcessing CSV {filename}\033[0m")
                filtered = []
                with open(local_path, "r", newline="") as infile:
                    reader = csv.reader(infile)
                    header = next(reader, None)
                    if header:
                        filtered.append(header)
                    for row in reader:
                        row_ids = [c for c in row if c in target_ids]
                        if row_ids:
                            for rid in row_ids:
                                if not id_status[rid]:
                                    print(f"✓ Detected {rid} in CSV {key}")
                                    id_status[rid] = True
                                detection_map[rid].add(db)
                            continue
                        filtered.append(row)
                with open(local_path, "w", newline="") as outfile:
                    writer = csv.writer(outfile)
                    writer.writerows(filtered)
                pbar.update(1)

                # Step 3: Upload
                rel = key[len(src_prefix):]
                new_key = dst_prefix + rel
                pbar.set_description(f"\033[1mUploading CSV {filename}\033[0m")
                try:
                    fsize = os.path.getsize(local_path)
                except OSError:
                    fsize = 0
                if fsize > 0:
                    cb_up = ProgressPercentage(filename, fsize)
                    try:
                        s3_client.upload_file(local_path, bucket, new_key, Callback=cb_up)
                    except botocore.exceptions.ClientError:
                        pass
                else:
                    try:
                        s3_client.upload_file(local_path, bucket, new_key)
                    except botocore.exceptions.ClientError:
                        pass
                os.remove(local_path)
                pbar.update(1)

            elif key.lower().endswith(".zip"):
                # ZIP: 5 steps
                try:
                    head = s3_client.head_object(Bucket=bucket, Key=key)
                    size = head.get("ContentLength", 0)
                except botocore.exceptions.ClientError:
                    size = 0

                zip_tmpdir = os.path.join(base_workdir, f"{db}_{os.path.splitext(filename)[0]}")
                os.makedirs(zip_tmpdir, exist_ok=True)
                local_zip = os.path.join(zip_tmpdir, "orig.zip")

                # Step 1: Download
                pbar.set_description(f"\033[1mDownloading ZIP {filename}\033[0m")
                if size > 0:
                    cb = ProgressPercentage(filename, size)
                    try:
                        s3_client.download_file(bucket, key, local_zip, Callback=cb)
                    except botocore.exceptions.ClientError:
                        for _ in range(5):
                            pbar.update(1)
                        continue
                else:
                    try:
                        s3_client.download_file(bucket, key, local_zip)
                    except botocore.exceptions.ClientError:
                        for _ in range(5):
                            pbar.update(1)
                        continue
                pbar.update(1)

                # Step 2: Extract
                pbar.set_description(f"\033[1mExtracting ZIP {filename}\033[0m")
                extract_dir = os.path.join(zip_tmpdir, "extracted")
                os.makedirs(extract_dir, exist_ok=True)
                try:
                    with zipfile.ZipFile(local_zip, "r") as zin:
                        zin.extractall(extract_dir)
                except zipfile.BadZipFile:
                    for _ in range(4):
                        pbar.update(1)
                    continue
                pbar.update(1)

                # Step 3: Process manifest
                pbar.set_description(f"\033[1mProcessing manifest {filename}\033[0m")
                manifest_path = None
                for root, _, files in os.walk(extract_dir):
                    if "SOURMASH-MANIFEST.csv" in files:
                        manifest_path = os.path.join(root, "SOURMASH-MANIFEST.csv")
                        break
                if manifest_path is None:
                    for _ in range(3):
                        pbar.update(1)
                    continue

                md5s_to_remove = set()
                with open(manifest_path, "r", encoding="utf-8", newline="") as mf:
                    try:
                        next(mf)
                    except StopIteration:
                        for _ in range(2):
                            pbar.update(1)
                        continue
                    reader = csv.DictReader(mf)
                    md5_col = next((c for c in reader.fieldnames if c.lower() == "md5"), None)
                    if md5_col is None:
                        for _ in range(2):
                            pbar.update(1)
                        continue
                    for row in reader:
                        name_field = row.get("name", "")
                        if not name_field:
                            continue
                        first_token = name_field.split(None, 1)[0]
                        if first_token in target_ids:
                            if not id_status[first_token]:
                                print(f"✓ Detected {first_token} in ZIP manifest of {key}")
                                id_status[first_token] = True
                            detection_map[first_token].add(db)
                            md5_val = row.get(md5_col)
                            if md5_val:
                                md5s_to_remove.add(md5_val)
                pbar.update(1)

                # Step 4: Rebuild with progress bar
                pbar.set_description(f"\033[1mRebuilding ZIP {filename}\033[0m")
                # Gather all files to zip
                files_to_zip = []
                for root, _, files in os.walk(extract_dir):
                    for fname in files:
                        full_path = os.path.join(root, fname)
                        arcname = os.path.relpath(full_path, extract_dir)
                        files_to_zip.append((full_path, arcname))
                # Show a progress bar over these files
                rebuild_pbar = tqdm(
                    files_to_zip,
                    desc=f"\033[2mRebuilding {filename}\033[0m",
                    leave=False,
                    unit="file"
                )
                rebuilt = os.path.join(zip_tmpdir, "rebuilt.zip")
                with zipfile.ZipFile(rebuilt, "w", compression=zipfile.ZIP_DEFLATED) as zout:
                    for full_path, arcname in rebuild_pbar:
                        zout.write(full_path, arcname=arcname)
                rebuild_pbar.close()
                pbar.update(1)

                # Step 5: Upload rebuilt ZIP
                rel = key[len(src_prefix):]
                new_key = dst_prefix + rel
                pbar.set_description(f"\033[1mUploading ZIP {filename}\033[0m")
                try:
                    fsize = os.path.getsize(rebuilt)
                except OSError:
                    fsize = 0
                if fsize > 0:
                    cb_up = ProgressPercentage(filename, fsize)
                    try:
                        s3_client.upload_file(rebuilt, bucket, new_key, Callback=cb_up)
                    except botocore.exceptions.ClientError:
                        pass
                else:
                    try:
                        s3_client.upload_file(rebuilt, bucket, new_key)
                    except botocore.exceptions.ClientError:
                        pass
                pbar.update(1)

                # Keep zip_tmpdir intact

            else:
                # Not CSV or ZIP: skip
                pbar.set_description("\033[1mSkipping non-CSV/ZIP\033[0m")
                continue

    # Close main bar (leave=False removes it)
    pbar.close()

    # 4) Print final table with vertical borders, centered ✓/✗

    db_list = sorted(databases)
    id_list = sorted(target_ids)

    # Column widths: first ID, then each database column
    id_w = max(len("ID"), max(len(t) for t in id_list)) + 2
    db_w = {db: max(len(db), 1) + 2 for db in db_list}

    # Header
    header_cells = [f"| {'ID':^{id_w}} "]
    for db in db_list:
        header_cells.append(f"| {db:^{db_w[db]}} ")
    header_cells.append("|")
    header_line = "".join(header_cells)

    # Separator
    sep_cells = [f"|{'-' * (id_w + 2)}"]
    for db in db_list:
        sep_cells.append(f"|{'-' * (db_w[db] + 2)}")
    sep_cells.append("|")
    separator_line = "".join(sep_cells)

    print("\n" + header_line)
    print(separator_line)

    # Rows
    for tid in id_list:
        row_cells = [f"| {tid:^{id_w}} "]
        for db in db_list:
            if db in detection_map[tid]:
                sym = f"\033[34m✓\033[0m"
            else:
                sym = f"\033[31m✗\033[0m"
            row_cells.append(f"| {sym:^{db_w[db]}} ")
        row_cells.append("|")
        print("".join(row_cells))

if __name__ == "__main__":
    main()
