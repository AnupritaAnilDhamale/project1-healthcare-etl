"""
generate_synthetic_data.py — Generate realistic healthcare test data
Healthcare ETL Pipeline | Anuprita Dhamale

Usage:
    python data/generate_synthetic_data.py --rows 500000
    python data/generate_synthetic_data.py --rows 10000 --output-dir ./test_data
"""

import argparse
import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
np.random.seed(42)

SPECIALTIES  = ["Cardiology","Orthopedics","Neurology","Oncology","Pediatrics",
                 "Dermatology","Radiology","Emergency Medicine","Family Medicine","Psychiatry"]
PLAN_TYPES   = ["HMO","PPO","EPO","POS"]
GENDERS      = ["M","F","NB","UNKNOWN"]
STATUSES     = ["APPROVED","DENIED","PENDING","APPEALED"]
DIAG_CODES   = [f"I{i:02d}" for i in range(1, 50)] + \
               [f"E{i:02d}" for i in range(1, 30)] + \
               [f"J{i:02d}" for i in range(1, 20)]
STATES       = ["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI"]


def generate_providers(n: int = 200) -> pd.DataFrame:
    records = []
    for i in range(n):
        records.append({
            "provider_id":   f"PRV{i:05d}",
            "provider_name": fake.name(),
            "specialty":     random.choice(SPECIALTIES),
            "npi":           str(random.randint(1_000_000_000, 9_999_999_999)),
            "state":         random.choice(STATES),
        })
    return pd.DataFrame(records)


def generate_patients(n: int = 50_000) -> pd.DataFrame:
    records = []
    for i in range(n):
        dob = fake.date_of_birth(minimum_age=0, maximum_age=95)
        records.append({
            "patient_id": f"PAT{i:07d}",
            "first_name": fake.first_name(),
            "last_name":  fake.last_name(),
            "dob":        dob.strftime("%Y-%m-%d"),
            "gender":     random.choices(GENDERS, weights=[48, 48, 2, 2])[0],
            "zip_code":   fake.zipcode(),
            "plan_type":  random.choices(PLAN_TYPES, weights=[30, 50, 10, 10])[0],
        })
    return pd.DataFrame(records)


def generate_claims(n: int, patient_ids: list, provider_ids: list) -> pd.DataFrame:
    records = []
    start = datetime(2022, 1, 1)
    end   = datetime(2024, 12, 31)
    date_range = (end - start).days

    for i in range(n):
        billed = round(random.lognormvariate(7.5, 1.2), 2)   # ~$1,800 median
        status = random.choices(STATUSES, weights=[70, 15, 10, 5])[0]
        paid   = round(billed * random.uniform(0.6, 0.95), 2) if status == "APPROVED" else 0.0

        records.append({
            "claim_id":       f"CLM{i:09d}",
            "patient_id":     random.choice(patient_ids),
            "provider_id":    random.choice(provider_ids),
            "claim_date":     (start + timedelta(days=random.randint(0, date_range))).strftime("%Y-%m-%d"),
            "diagnosis_code": random.choice(DIAG_CODES),
            "billed_amount":  billed,
            "paid_amount":    paid,
            "claim_status":   status,
        })

    # Inject ~1% nulls to simulate real-world messiness
    df = pd.DataFrame(records)
    null_mask = np.random.random(len(df)) < 0.01
    df.loc[null_mask, "diagnosis_code"] = np.nan
    df.loc[null_mask[:len(df)//2], "paid_amount"] = np.nan

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic healthcare data")
    parser.add_argument("--rows",       type=int, default=500_000, help="Number of claim rows")
    parser.add_argument("--output-dir", type=str, default="./data/synthetic",  help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Generating {args.rows:,} synthetic healthcare records...")

    providers = generate_providers(n=200)
    patients  = generate_patients(n=min(50_000, args.rows // 10))
    claims    = generate_claims(
        n=args.rows,
        patient_ids=patients["patient_id"].tolist(),
        provider_ids=providers["provider_id"].tolist(),
    )

    providers.to_csv(f"{args.output_dir}/providers.csv", index=False)
    patients.to_csv( f"{args.output_dir}/patients.csv",  index=False)
    claims.to_csv(   f"{args.output_dir}/claims.csv",    index=False)

    print(f"✅ Done!")
    print(f"   providers.csv — {len(providers):,} rows")
    print(f"   patients.csv  — {len(patients):,}  rows")
    print(f"   claims.csv    — {len(claims):,}   rows")
    print(f"   Saved to: {args.output_dir}/")


if __name__ == "__main__":
    main()
