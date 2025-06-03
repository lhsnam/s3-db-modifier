# S3 Cleanup Tool

A lightweight Python script to remove specified IDs from CSVs and ZIPs under an S3 prefix, then upload cleaned files to a new prefix. Perfect for bulk‐removal of accession IDs (e.g., `GCA_013364925.1`) across thousands of Sourmash database files.

## Quick Start

1. **Install dependencies** (Python 3.8+):

   ```bash
   git clone https://github.com/YourUser/s3-cleanup-tool.git
   cd s3-cleanup-tool
   pip install -r requirements.txt
   ```
2. **Configure AWS credentials** (via `~/.aws/credentials` or environment vars).
3. **Run the script**:

   ```bash
   src/remove_id_and_publish.py \
     -i ID1,ID2,ID3 \
     --src-prefix sourmash-databases/k21/ \
     --dst-prefix sourmash-databases-2506/k21/ \
     [--exclude-db virus,bact] \
     [--region us-east-1] \
     [--workdir ./WORK]
   ```

   * `-i`: comma‐separated IDs to remove.
   * `--src-prefix`: S3 prefix containing original CSVs/ZIPs.
   * `--dst-prefix`: S3 prefix where cleaned files will be written.
   * `--exclude-db`: skip any database (subfolder) whose name contains these substrings.
   * `--region`: AWS region (optional).
   * `--workdir`: local directory for ZIP extraction (defaults to `./WORK`).

## Summary of Behavior

* **CSV files**: downloads → removes rows where “name” matches any ID → re‐uploads.
* **ZIP files**: downloads → extracts → reads `SOURMASH-MANIFEST.csv` to find matching IDs → deletes related signature files → re‐zips (with a mini progress bar) → re‐uploads.
* **Progress bars**: one bold bar for total steps, dim bars for each file’s byte transfers.
* **Final table**: shows “✓” (blue) if an ID was cleaned in a specific database, “✗” (red) otherwise.

That’s it—clone, configure AWS, and run with your list of IDs. Cleaned files are safely written under a new prefix, never overwriting the original `sourmash-databases/` content.
