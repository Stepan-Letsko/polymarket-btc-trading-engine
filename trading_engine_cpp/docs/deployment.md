# Deployment

How the recording server is set up, from an empty AWS account to
recordings arriving in Google Cloud Storage. Every command here is one
that was actually run on the current server, in order, with the reason
for it.

How to use each program (options, output, exit codes) is in
[programs.md](programs.md); this file is about running them as a
permanent service.

**Placeholders.** Account-specific identifiers are kept out of the repo.
Replace these with the real values from your own notes:

| Placeholder | What it is | Where to find it |
|---|---|---|
| `<AWS_ACCOUNT_ID>` | 12-digit AWS account number | AWS console, top right under your name |
| `<GCP_PROJECT_ID>` | Google Cloud project ID | project picker at the top of the Google console |
| `<GCP_PROJECT_NUMBER>` | the project's numeric ID | `gcloud projects describe <GCP_PROJECT_ID> --format='value(projectNumber)'` |
| `<BUCKET>` | Cloud Storage bucket name | Cloud Storage → Buckets |
| `<PUBLIC_IP>` | the server's public IP | EC2 → Instances |

---

## 1. Overview

```
 binance_ws ─┐                                            ┌─ Google Cloud Storage
 clob_ws     ├─► one .jsonl per feed per UTC hour ─► compress_hours ─► .zst ─► (upload job, to come)
 polybolt_ws ┘   /var/lib/polyrecorder/data/<feed>/     every 10 min                  gs://<BUCKET>
```

- **Three recorders**, each a systemd service running permanently and
  restarted 2 seconds after any crash.
- **compress-hours**, a systemd timer: every 10 minutes it compresses
  every finished hour to `.zst` (zstd level 19), verifies it, and only
  then deletes the `.jsonl`.
- **Upload to Google Cloud Storage** (section 8, not built yet): the
  server proves who it is with Workload Identity Federation, so it holds
  no Google password or key.

Data volume, measured on 2026-11-10:

| Feed | Raw per hour | Compressed per hour |
|---|---|---|
| clob | ~1.6 GB | ~120 MB (13×) |
| binance | ~180–300 MB | ~7–12 MB (25×) |
| polybolt | ~2 MB | ~0.14 MB |

About **3.2 GB a day** compressed. The 58 GB disk (50 GB free) holds
about two weeks, which is why the upload-and-cleanup job is needed.

---

## 2. The server (AWS)

| Setting | Value | Why |
|---|---|---|
| Region | **eu-west-2 (London)** | Polymarket's servers are in eu-west-2. Measured round trip to `clob.polymarket.com`: median 20.5 ms, against 24.3 ms from Google London. |
| Availability zone | eu-west-2c | AZ *names* are shuffled per AWS account; the real identity is the AZ ID (`euw2-azN`), shown under EC2 → Settings → Zones. |
| Instance type | **c7i.large** (2 vCPU, 4 GB) | Steady compute, no burst credits to run out. 2 vCPU = 1 physical core with hyperthreading. |
| OS | **Ubuntu Server 26.04 LTS** | |
| Disk | **gp3, 60 GB, encrypted** | |
| Key pair | **ED25519**, `.pem` | |
| Termination protection | on | So the instance can't be deleted by one wrong click. |
| Security group | **SSH (22) from "My IP" only** | Nothing else is open. Update it when your home IP changes. |

Connect from the Mac:

```bash
mkdir -p ~/.ssh && chmod 700 ~/.ssh
mv ~/Downloads/PolyRecorder.pem ~/.ssh/
chmod 400 ~/.ssh/PolyRecorder.pem          # ssh refuses keys others can read
ssh -i ~/.ssh/PolyRecorder.pem ubuntu@<PUBLIC_IP>
```

Never put a `.pem` in the repo (`*.pem` is git-ignored).

### Clock

Ubuntu on EC2 runs **chrony** synced to the **Amazon Time Sync
Service** (`169.254.169.123`), typically within a few microseconds.
Check it with:

```bash
timedatectl          # "System clock synchronized: yes"
chronyc tracking     # "System time" = current offset
```

---

## 3. First setup

### 3.1 Build and test

```bash
sudo apt update
sudo apt install -y build-essential cmake git pkg-config libssl-dev zlib1g-dev
git clone https://github.com/Stepan-Letsko/polymarket-btc-trading-engine.git
cd polymarket-btc-trading-engine/trading_engine_cpp
mkdir -p build && cd build
cmake ..
cmake --build . -j2
./recorder_test && ./backoff_test && ./hourly_file_sink_test && ./compress_file_test && ./compress_hours_test
```

All five must print `all checks passed`.

### 3.2 The service account

The services run as `polyrecorder`, a **system account**: no password,
no home folder, no login shell. It can write the data folder and
nothing else, so a bug in a recorder can't touch the rest of the
machine.

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin polyrecorder

# Let the ubuntu login read the recordings (log out and back in after)
sudo usermod -aG polyrecorder ubuntu
```

### 3.3 Folders

| Folder | Owner | Mode | Holds |
|---|---|---|---|
| `/opt/polyrecorder/bin` | root | 755 | the programs. Root-owned, so the services can run them but not change them. |
| `/var/lib/polyrecorder/data` | polyrecorder | 750 | the recordings, one subfolder per feed |
| `/etc/polyrecorder` | root | 700 | settings: the PolyBolt credentials and the Google settings file |

```bash
sudo install -d -o root         -g root         -m 755 /opt/polyrecorder/bin
sudo install -d -o polyrecorder -g polyrecorder -m 750 /var/lib/polyrecorder /var/lib/polyrecorder/data
sudo install -d -o root         -g root         -m 700 /etc/polyrecorder
```

### 3.4 Install the programs

From `trading_engine_cpp/build`:

```bash
sudo install -o root -g root -m 755 binance_ws clob_ws polybolt_ws compress_hours /opt/polyrecorder/bin/
```

### 3.5 PolyBolt credentials

`/etc/polyrecorder/polybolt.env` holds **only** `POLY_API_KEY`,
`POLY_SECRET` and `POLY_PASSPHRASE`, one `NAME=value` per line. **Never
the wallet's private key**: the recording server doesn't trade and must
not hold it.

```bash
nano ~/polybolt.env                      # type the three lines
sudo install -o root -g root -m 600 ~/polybolt.env /etc/polyrecorder/polybolt.env
rm ~/polybolt.env

# Check the names only - never print the values
sudo cut -d= -f1 /etc/polyrecorder/polybolt.env
```

Root-only (600): systemd reads it as root when starting the service and
passes the values in, so even the `polyrecorder` account can't open it.

### 3.6 Wait for the clock at boot

```bash
sudo systemctl enable chrony-wait.service
```

The recorders start after `time-sync.target`. That target only actually
waits for chrony when `chrony-wait.service` is enabled. Without it, the
first records after a reboot could be stamped by an unsynced clock.

---

## 4. systemd

The unit files are in [`deploy/systemd/`](../deploy/systemd/). Each one
explains its settings in comments.

| Unit | Kind | What it does |
|---|---|---|
| `binance-recorder.service` | runs permanently | `binance_ws --out-dir /var/lib/polyrecorder/data` |
| `clob-recorder.service` | runs permanently | `clob_ws --out-dir …` |
| `polybolt-recorder.service` | runs permanently | `polybolt_ws --out-dir …`, with the credentials from `polybolt.env` |
| `compress-hours.service` | one pass, then exits | `compress_hours --data-dir …` at idle priority |
| `compress-hours.timer` | schedule | starts `compress-hours.service` 5 min after boot, then every 10 min |

### 4.1 Install

From `trading_engine_cpp/deploy/systemd`:

```bash
sudo install -m 644 binance-recorder.service clob-recorder.service polybolt-recorder.service \
                    compress-hours.service compress-hours.timer /etc/systemd/system/
sudo systemctl daemon-reload

sudo systemctl enable --now binance-recorder clob-recorder polybolt-recorder
sudo systemctl enable --now compress-hours.timer      # the timer, not the service
```

- `daemon-reload` makes systemd re-read the unit files.
- `enable` starts the unit at every boot, and `--now` starts it
  straight away as well.

### 4.2 Check

```bash
systemctl status binance-recorder clob-recorder polybolt-recorder
systemctl list-timers compress-hours.timer             # last and next pass
journalctl -u clob-recorder -f                         # follow a recorder's log
journalctl -u compress-hours --since today             # what each pass did
ls -lh /var/lib/polyrecorder/data/*/                   # the files themselves
```

### 4.3 Memory protection

The machine has 3.7 GB and **no swap**. If memory runs out, the
kernel's OOM killer kills a process to survive. Two settings make sure
that's never a recorder:

- **`OOMScoreAdjust=-500`** on the three recorders: the OOM killer
  picks almost anything else first. It's not -1000 ("never"), so a
  recorder that leaked memory would still be the one killed (and
  restarted), rather than SSH.
- **`MemoryMax=1G`** on compress-hours: a hard cap. Over it, only the
  pass is killed. That's harmless: it leaves at most a `.tmp`, which
  the next pass overwrites.

A changed `OOMScoreAdjust` only applies when a recorder starts. To
apply it to running recorders without a restart (a restart costs a
couple of seconds of data):

```bash
for p in binance_ws clob_ws polybolt_ws; do sudo choom -p "$(pidof $p)" -n -500; done
```

Check:

```bash
systemctl show -p OOMScoreAdjust binance-recorder clob-recorder polybolt-recorder
systemctl show -p MemoryMax compress-hours
for p in binance_ws clob_ws polybolt_ws; do echo "$p $(cat /proc/$(pidof $p)/oom_score_adj)"; done
```

Memory in use (RSS, in KB). A recorder should stay around 10–12 MB:

```bash
ps -o pid,rss,etime,comm -C clob_ws,binance_ws,polybolt_ws
```

### 4.4 Updating

**New unit files** (after `git pull`): repeat the `install` and
`daemon-reload` from 4.1. Changes to a recorder's settings take effect
at its next start.

**New programs:** rebuild and test (3.1), install (3.4), then restart
the recorders:

```bash
sudo systemctl restart binance-recorder clob-recorder polybolt-recorder
```

A restart costs a couple of seconds of data, so do it when that
matters least. Nothing needs restarting for a new `compress_hours`:
the next pass uses it.

---

## 5. Compression level

`compress_hours` uses **zstd level 19**. It was chosen from a full real
hour of clob (1596 MB, the hardest feed), measured on this server on
2026-11-10:

| Setting | Compressed | Time | Memory |
|---|---|---|---|
| 3 | 165.4 MB | 6 s | 39 MB |
| 9 | 150.7 MB | 15 s | 82 MB |
| 15 | 145.0 MB | 69 s | 137 MB |
| **19** | **120.4 MB** | **14 min** | **221 MB** |
| 22 (`--ultra`) | 121.0 MB | 29 min | 2.36 GB |
| 19 + `--long=27` | 120.5 MB | 14 min | 490 MB |

- **19 is the smallest result.** 22 came out slightly bigger, at twice
  the time and ten times the memory. Long mode found nothing to add, so
  the repetition in clob is all recent.
- **It costs about 14 CPU-minutes per hour of data**, roughly a quarter
  of one vCPU, at idle priority.

How to measure, if the data ever changes. Stream with `-c`, never
benchmark mode `-b`: `-b` loads the whole file into memory and got
OOM-killed at 3.5 GB.

```bash
zstd -d /var/lib/polyrecorder/data/clob/<some hour>.jsonl.zst -o ~/bench/clob.jsonl
for opt in "-3" "-9" "-15" "-19" "--ultra -22" "-19 --long=27"; do
  echo "=== $opt"
  /usr/bin/time -f "%e s, %M KB peak memory" nice -n 19 zstd -q $opt -c ~/bench/clob.jsonl | wc -c
done
```

---

## 6. AWS → Google Cloud Storage link

### 6.1 How it works

The server holds **no Google password or key**. Google instead trusts
AWS's word for who the server is (**Workload Identity Federation**):

1. The upload tool gets the server's temporary AWS credentials from the
   AWS metadata service. They belong to the IAM role attached to the
   instance.
2. It signs an AWS "who am I?" request with them and sends it to
   Google's Security Token Service (STS).
3. Google forwards it to AWS. AWS answers "account <AWS_ACCOUNT_ID>, role
   `polyrecorder-gcs-uploader`".
4. Google checks that the identity is on the pool's guest list, and
   returns a pass valid for 1 hour.

A key file was the alternative. It's blocked by default on new Google
organizations (the `iam.disableServiceAccountKeyCreation` policy), and
the policy is still on.

### 6.2 Bucket

| Setting | Value |
|---|---|
| Name | `<BUCKET>` |
| Project | `<GCP_PROJECT_ID>` (number `<GCP_PROJECT_NUMBER>`) |
| Location | Region, **europe-west2 (London)** |
| Storage class | Standard |
| Hierarchical namespace | off |
| Public access prevention | enforced |
| Access control | uniform |
| Soft delete | 7 days (default) |
| Object versioning | off |
| Retention policy | none |
| Encryption | Google-managed |

**Layout in the bucket:**
`<feed>/date=YYYY-MM-DD/<feed>_YYYY-MM-DDTHH.jsonl.zst`. The `date=`
folders are the "Hive-style" convention: BigQuery, pandas and pyarrow
read them as a `date` column and can skip whole days.

### 6.3 AWS side: the role

The role is the server's identity. It has **no AWS permissions**.

1. **IAM → Roles → Create role**: trusted entity **AWS service**, use
   case **EC2**. Select no permissions. Name it
   `polyrecorder-gcs-uploader`.
2. **EC2 → Instances →** the server **→ Actions → Security → Modify IAM
   role →** `polyrecorder-gcs-uploader`. This takes effect immediately,
   with no restart.

Check from the server:

```bash
TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/iam/info; echo
```

You should see `"Code" : "Success"` and `instance-profile/polyrecorder-gcs-uploader`.

Never query the metadata path `iam/security-credentials/…` by hand: it
prints the live (secret) temporary credentials.

### 6.4 Google side: pool, provider, permissions

In Cloud Shell, as one block:

```bash
gcloud config set project <GCP_PROJECT_ID>
PROJECT_NUMBER=$(gcloud projects describe <GCP_PROJECT_ID> --format='value(projectNumber)')
ROLE_ARN="arn:aws:sts::<AWS_ACCOUNT_ID>:assumed-role/polyrecorder-gcs-uploader"

# Identity and token-exchange services
gcloud services enable iam.googleapis.com sts.googleapis.com

# The guest list
gcloud iam workload-identity-pools create aws-recorder \
  --location=global --display-name="AWS recording server"

# Trust AWS account <AWS_ACCOUNT_ID>, but only the uploader role
gcloud iam workload-identity-pools providers create-aws aws-account \
  --location=global --workload-identity-pool=aws-recorder \
  --account-id=<AWS_ACCOUNT_ID> \
  --attribute-condition="attribute.aws_role == '$ROLE_ARN'"

# Add and read on this bucket only: no delete, so no overwrite either
MEMBER="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/aws-recorder/attribute.aws_role/$ROLE_ARN"
gcloud storage buckets add-iam-policy-binding gs://<BUCKET> \
  --member="$MEMBER" --role=roles/storage.objectCreator
gcloud storage buckets add-iam-policy-binding gs://<BUCKET> \
  --member="$MEMBER" --role=roles/storage.objectViewer

# The settings file for the server
gcloud iam workload-identity-pools create-cred-config \
  projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/aws-recorder/providers/aws-account \
  --aws --enable-imdsv2 --output-file=gcs-wif.json
cat gcs-wif.json
```

`gcs-wif.json` contains **no secret**, only addresses: where to get AWS
credentials, and where to exchange them. It's useless on any machine
without the role.

### 6.5 Server side

Install the Google Cloud CLI from Google's package source, so `apt
upgrade` keeps it current:

```bash
sudo apt-get install -y ca-certificates gnupg curl
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install -y google-cloud-cli
```

Put the settings file in place by pasting the `cat` output from 6.4
between the `EOF` lines:

```bash
sudo tee /etc/polyrecorder/gcs-wif.json > /dev/null <<'EOF'
<contents of gcs-wif.json>
EOF
sudo chmod 644 /etc/polyrecorder/gcs-wif.json
```

`CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE` points `gcloud` at the file,
so there's no login step and no saved state.

### 6.6 Test

```bash
export WIF=/etc/polyrecorder/gcs-wif.json

# Must work
sudo CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=$WIF gcloud storage ls -l -r gs://<BUCKET>
sudo CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=$WIF gcloud storage cp --no-clobber \
  /var/lib/polyrecorder/data/polybolt/<an hour>.jsonl.zst gs://<BUCKET>/polybolt/date=<day>/
```

The next two must **fail** with `403 … storage.objects.delete`. They
prove the server can't overwrite or delete anything in the bucket:

```bash
sudo CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=$WIF gcloud storage cp \
  /var/lib/polyrecorder/data/polybolt/<same hour>.jsonl.zst gs://<BUCKET>/polybolt/date=<day>/
sudo CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=$WIF gcloud storage rm \
  gs://<BUCKET>/polybolt/date=<day>/<same hour>.jsonl.zst
```

Uploads are checked with **CRC32C**: a fingerprint of every byte. Any
damage in transit gives a different value, and the upload fails instead
of storing a corrupted file.

---

## 7. Moving to a new server

1. Launch it as in section 2, in the same AWS account.
2. Run section 3 (build, account, folders, programs, credentials, chrony-wait).
3. Run section 4.1 (units).
4. Attach the `polyrecorder-gcs-uploader` role (6.3, step 2).
5. Run section 6.5 (CLI and settings file) and 6.6 (test).

The Google side (6.4) needs no change for a server in the same AWS
account. For a different AWS account, the provider must trust the new
account ID: create a new provider, or update the existing one.

Before stopping the old server, let it compress and upload its last
hours.

---

## 8. Upload job

Not built yet. The plan: an `upload-hours` service and timer, running as
`polyrecorder`. Each pass:

1. Lists the bucket once, with each file's CRC32C.
2. Uploads every local `.zst` file the bucket lacks, with `--no-clobber`.
3. Deletes a local `.zst` file only when the bucket holds it with the
   same CRC32C **and** it is older than 7 days.
