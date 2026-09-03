"""
faker_source/generate_merchants.py

Generates synthetic merchant risk-score records (a simulated compliance/risk
feed) for the real merchant names found in the transaction dataset, and
writes them as JSON batch files into a watched dropzone/ folder for NiFi to
pick up via GetFile.

This does NOT duplicate fields already present in the transaction data
(category, location) - it only adds genuinely new reference information:
risk_score, blacklist status, and compliance status.
"""

import pandas as pd
import json
import time
import os
import random
from datetime import datetime, timedelta
from faker import Faker

# ---- Config ----
CSV_PATH = "C:/Final_Capstone_Project/producer/data/fraudTrain_clean.csv"
DROPZONE_DIR = "faker_source/dropzone"

BATCH_INTERVAL_SECONDS = 1   # how often to write a new batch file
NUM_BATCHES = 10              # how many batches to generate before stopping
MERCHANTS_PER_BATCH = 50      # how many merchants get an updated score per batch

fake = Faker()
os.makedirs(DROPZONE_DIR, exist_ok=True)

# ---- Load the real merchant names from the transaction dataset ----
df = pd.read_csv(CSV_PATH, usecols=["merchant"])
all_merchants = df["merchant"].unique().tolist()
print(f"Loaded {len(all_merchants)} unique merchants from transaction data.")


def generate_merchant_record(merchant_name: str) -> dict:
    """Build one synthetic risk record for a real merchant name."""
    is_flagged = fake.boolean(chance_of_getting_true=20)
    return {
        "merchant": merchant_name,  # must exactly match the 'merchant' field in transactions - join key
        "risk_score": random.randint(0, 100),
        "is_blacklisted": fake.boolean(chance_of_getting_true=5),
        "last_flagged_date": fake.date_this_year().isoformat() if is_flagged else None,
        "compliance_status": random.choice(["clear", "flagged", "under_review"]),
        "last_updated": datetime.now().isoformat()
    }


# Replace the random.sample() approach with deterministic full coverage,
# split across batches so it still simulates periodic updates.

def write_batch(batch_num: int, merchants_chunk: list):
    records = [generate_merchant_record(m) for m in merchants_chunk]
    filename = f"merchant_batch_{batch_num}_{int(time.time())}.json"
    filepath = os.path.join(DROPZONE_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Wrote batch {batch_num}: {len(records)} records -> {filepath}")

# Split ALL merchants into NUM_BATCHES roughly-equal chunks, so every
# merchant gets covered exactly once across the full run.
chunk_size = (len(all_merchants) // NUM_BATCHES) + 1
for batch_num in range(1, NUM_BATCHES + 1):
    start = (batch_num - 1) * chunk_size
    chunk = all_merchants[start : start + chunk_size]
    if not chunk:
        break
    write_batch(batch_num, chunk)
    if batch_num < NUM_BATCHES:
        time.sleep(BATCH_INTERVAL_SECONDS)
        
print(f"Done. Generated {NUM_BATCHES} batches into '{DROPZONE_DIR}'.")