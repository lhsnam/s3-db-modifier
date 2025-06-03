# S3 Cleanup Tool

A simple Python script to remove specified IDs from CSVs and ZIPs stored under an S3 prefix, then re-upload the “cleaned” files to a new prefix—complete with progress bars, optional database exclusion, and a final summary table.

---

## Why This Exists

In our Sourmash database pipeline, we store millions of CSV and ZIP files on S3 under folders like `sourmash-databases/k21/...`. Sometimes, regulatory or quality‐control reasons require us to remove every occurrence of a particular accession ID (e.g. `GCA_013364925.1`) across all files. Doing this manually is error‐prone and extremely slow.

This tool automates that process:

* **Scans every object** under a given S3 prefix
* **Filters out rows in CSV files** where the “name” column matches any input ID
* **Extracts ZIP files, removes matching signatures**, re‐zips, and re‐uploads
* **Tracks progress** with a single bold progress bar for all sub‐steps, plus dim, byte‐level bars for each download/upload
* **Prints a final table** showing, for each input ID and each “database” (subfolder), whether it was cleaned (“✓”) or not (“✗”)
* **Optionally excludes entire databases** (subfolders) by substring, so you can skip, for example, anything that contains “virus” or “bact”

---

## Table of Contents

1. [Features](#features)
2. [Installation](#installation)
3. [Usage](#usage)

   * [Basic Command](#basic-command)
   * [Arguments](#arguments)
   * [Examples](#examples)
4. [How It Works (High Level)](#how-it-works-high-level)
5. [Directory Layout](#directory-layout)
6. [Dependencies](#dependencies)
7. [License](#license)

---

## Features

* **ID removal from CSVs**

  * Downloads each CSV, strips out rows where any column equals one of the input IDs, and re‐uploads the filtered CSV.
* **ID removal from ZIPs**

  * Downloads each ZIP, extracts to a local “WORK” folder (or user‐specified `--workdir`), reads `SOURMASH-MANIFEST.csv` to find matching IDs, removes related signature files, re‐zips, and re‐uploads.
* **Single bold progress bar**

  * Tracks all sub‐steps (download, process, upload) across every file.
* **Dimmed byte‐level progress bars**

  * For each individual download/upload, so you can see bandwidth and file size at a glance.
* **Optional exclusion**

  * Skip any “database” (subfolder) whose name contains a user‐provided substring (e.g. `--exclude-db virus,bact`).
* **Final summary table**

  * Lists each requested ID as a row, each “database” as a column, and shows “✓” (blue) if the ID was cleaned from that database, or “✗” (red) if not found there.

---

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/YourUser/s3-cleanup-tool.git
   cd s3-cleanup-tool
   ```

2. **(Optional) Create a virtual environment**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Make the script executable** (if not already)

   ```bash
   chmod +x src/remove_id_and_publish.py
   ```

5. **Ensure AWS credentials are configured**
   Either export environment variables:

   ```bash
   export AWS_ACCESS_KEY_ID=<your_key>
   export AWS_SECRET_ACCESS_KEY=<your_secret>
   export AWS_DEFAULT_REGION=us-east-1
   ```

   Or set up `~/.aws/credentials` with a named profile.

---

## Usage

### Basic Command

```bash
src/remove_id_and_publish.py \
  -i ID1,ID2,ID3 \
  --src-prefix sourmash-databases/k21/ \
  --dst-prefix sourmash-databases-2506/k21/ \
  [--exclude-db virus,bact] \
  [--region us-east-1] \
  [--workdir /path/to/WORK]
```

### Arguments

* `-i, --ids` (required)
  Comma‐separated list of accession IDs to remove.
  Example: `-i GCA_013364925.1,GCA_005768625.2`

* `--exclude-db` (optional)
  Comma‐separated list of substrings. Any database (first-level subfolder under `k21/`, `k31/`, or `k51/`) containing one of these substrings is skipped entirely.
  Example: `--exclude-db virus,bact` will skip any database folder whose name includes “virus” or “bact.”

* `--bucket` (optional, default `io-pipeline-references`)
  Name of the S3 bucket containing your Sourmash database prefixes.

* `--src-prefix` (optional, default `sourmash-databases/k21/`)
  S3 prefix to scan. All objects (CSV/ZIP) under this prefix will be processed unless excluded.

* `--dst-prefix` (optional, default `sourmash-databases-2506/k21/`)
  S3 prefix to which “cleaned” CSVs and rebuilt ZIPs will be uploaded.
  **Safety check**: this cannot be `sourmash-databases/` (the original root), nor nested inside the source prefix.

* `--region` (optional)
  AWS region for the S3 client (e.g. `us-east-1`). If omitted, the default boto3 region is used.

* `--workdir` (optional, default `./WORK`)
  Local directory for extracting ZIP files and rebuilding them. If not provided, a folder named `WORK` is created in the current directory.

* `-h, --help`
  Show a brief help message and exit.

### Examples

1. **Remove two IDs from every CSV/ZIP under `k21/` and write to `k21-cleaned/`**

   ```bash
   src/remove_id_and_publish.py \
     -i GCA_013364925.1,GCA_005768625.2 \
     --src-prefix sourmash-databases/k21/ \
     --dst-prefix sourmash-databases-2506/k21/
   ```

2. **Exclude any database whose name contains “virus”**

   ```bash
   src/remove_id_and_publish.py \
     -i GCA_020087015.1 \
     --exclude-db virus \
     --src-prefix sourmash-databases/k21/ \
     --dst-prefix sourmash-databases-2506/k21/
   ```

3. **Specify a custom WORK folder for extraction**

   ```bash
   src/remove_id_and_publish.py \
     -i GCA_912686445.1 \
     --workdir /scratch/sourmash-work/ \
     --src-prefix sourmash-databases/k31/ \
     --dst-prefix sourmash-databases-2506/k31/
   ```

---

## How It Works (High Level)

1. **List all objects** under `--src-prefix` using the S3 paginator.
2. **Filter out** any “database” (first‐level subfolder) whose name matches `--exclude-db`.
3. **Count** how many CSVs and ZIPs remain, so we know how many total steps (CSV = 3 steps, ZIP = 5 steps).
4. **Initialize a bold main progress bar** with `<num_csv * 3 + num_zip * 5>` total steps.
5. **Iterate through each filtered key**:

   * If it’s a `.csv`:

     1. **Download** with a dim, byte‐level tqdm bar.
     2. **Read & filter**: remove rows where any column equals one of the input IDs.
     3. **Re‐upload** the filtered CSV with another byte‐level tqdm bar.
     4. After processing each CSV, advance the main bar by 3 steps.
   * If it’s a `.zip`:

     1. **Download** with byte‐level progress.
     2. **Extract** into a local folder `$WORK/<database>_<zipname>/extracted`.
     3. **Open** `SOURMASH-MANIFEST.csv` inside: for each row, parse `name`, check first token against target IDs. If matched, record `md5`; delete the corresponding signature files (`.sig`) whose filenames contain those MD5s.
     4. **Rebuild**: walk through every remaining file under `extracted/` and write them into `rebuilt.zip`, showing a small file‐level progress bar over each file being zipped.
     5. **Upload** the rebuilt ZIP to `--dst-prefix` with byte‐level progress.
     6. Advance the main bar by 5 steps.
6. **After all keys** are processed, the main progress bar closes (it vanishes cleanly).
7. **Print a final summary table**:

   * Rows = each requested ID
   * Columns = each “database” (sorted lexically)
   * Cell = “✓” in blue if that ID was detected (and cleaned) in that database, else “✗” in red

---

## Directory Layout

```
s3-cleanup-tool/
├── README.md                  # You are here
├── LICENSE                    # MIT or Apache 2.0, etc.
├── requirements.txt           # pinned dependencies (boto3, tqdm, etc.)
├── src/
│   └── remove_id_and_publish.py   # main script
│   └── utils.py                    # any helper functions (optional)
└── WORK/                      # default local working folder (gitignored)
    └── <database>_<zipname>/   # each ZIP’s extracted & rebuilt subfolder
```

---

## Dependencies

Pinned in `requirements.txt` (example):

```
boto3>=1.28.0
tqdm>=4.65.0
```

* Python 3.8+
* AWS credentials (either in `~/.aws/credentials` or via environment variables)
* IAM permissions:

  * `s3:ListBucket` on `io-pipeline-references`
  * `s3:GetObject` and `s3:PutObject` on the relevant prefixes

---

## License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.

---

## Acknowledgments

* The [boto3](https://github.com/boto/boto3) library for S3 interactions
* The [tqdm](https://github.com/tqdm/tqdm) package for progress bars

---

Thank you for using this tool! If you run into any issues or want to contribute, please open an issue or pull request on GitHub.

