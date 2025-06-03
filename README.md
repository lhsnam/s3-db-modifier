# S3 Cleanup Tool

A standalone Python script to remove specified IDs from CSVs and ZIPs under an S3 prefix and upload cleaned files to a new prefix.

## Usage

1. **Save the Python script** (e.g., `remove_id_and_publish.py`) to your local machine.
2. **Install required packages** (once) with:

   ```bash
   pip install boto3 tqdm
   ```
3. **Ensure AWS credentials** are set (via `~/.aws/credentials` or environment variables).
4. **Run the script** directly:

   ```bash
   python3 remove_id_and_publish.py \
     -i ID1,ID2,ID3 \
     --src-prefix sourmash-databases/k21/ \
     --dst-prefix sourmash-databases-2506/k21/ \
     [--exclude-db virus,bact] \
     [--region us-east-1] \
     [--workdir ./WORK]
   ```

**Arguments:**

* `-i`: comma-separated list of IDs to remove.
* `--src-prefix`: S3 prefix where original CSVs/ZIPs reside.
* `--dst-prefix`: S3 prefix for cleaned files.
* `--exclude-db` (optional): comma-separated substrings; skip databases whose names contain these.
* `--region` (optional): AWS region.
* `--workdir` (optional): local folder for ZIP extraction (defaults to `./WORK`).

That’s it—just the script and this README. No extra files needed.
