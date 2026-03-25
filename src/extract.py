"""
extract.py — S3 Ingestion + Schema Validation
Healthcare ETL Pipeline | Anuprita Dhamale
"""

import boto3
import pandas as pd
import logging
from io import StringIO
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Schema Definitions ────────────────────────────────────────────────────────
SCHEMA = {
    "claims": {
        "claim_id":       str,
        "patient_id":     str,
        "provider_id":    str,
        "claim_date":     "datetime",
        "diagnosis_code": str,
        "billed_amount":  float,
        "paid_amount":    float,
        "claim_status":   str,
    },
    "patients": {
        "patient_id":   str,
        "first_name":   str,
        "last_name":    str,
        "dob":          "datetime",
        "gender":       str,
        "zip_code":     str,
        "plan_type":    str,
    },
    "providers": {
        "provider_id":   str,
        "provider_name": str,
        "specialty":     str,
        "npi":           str,
        "state":         str,
    }
}

VALID_STATUSES   = {"APPROVED", "DENIED", "PENDING", "APPEALED"}
VALID_PLAN_TYPES = {"HMO", "PPO", "EPO", "POS"}


class S3Extractor:
    """Reads CSV files from S3, validates schema, enforces types."""

    def __init__(self, bucket: str, prefix: str = "raw/"):
        self.s3     = boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix

    def list_files(self, dataset: str) -> list[str]:
        response = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=f"{self.prefix}{dataset}/"
        )
        return [obj["Key"] for obj in response.get("Contents", [])]

    def read_csv(self, key: str) -> pd.DataFrame:
        obj  = self.s3.get_object(Bucket=self.bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        return pd.read_csv(StringIO(body))

    def validate_schema(self, df: pd.DataFrame, dataset: str) -> pd.DataFrame:
        """Enforce column presence, types, and business rules."""
        schema     = SCHEMA[dataset]
        missing    = set(schema.keys()) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in {dataset}: {missing}")

        for col, dtype in schema.items():
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype, errors="ignore")

        return df

    def quality_check(self, df: pd.DataFrame, dataset: str) -> dict:
        """Return a dict of quality metrics."""
        results = {
            "dataset":        dataset,
            "total_rows":     len(df),
            "null_pct":       (df.isnull().sum().sum() / df.size * 100).round(2),
            "duplicate_rows": df.duplicated().sum(),
            "checked_at":     datetime.utcnow().isoformat(),
        }

        if dataset == "claims":
            results["invalid_status"] = (~df["claim_status"].isin(VALID_STATUSES)).sum()
            results["negative_amounts"] = (df["billed_amount"] < 0).sum()

        if dataset == "patients":
            results["invalid_plan"] = (~df["plan_type"].isin(VALID_PLAN_TYPES)).sum()

        return results

    def extract(self, dataset: str) -> tuple[pd.DataFrame, dict]:
        """Full extract: read all S3 files → validate → quality check."""
        logger.info(f"Extracting dataset: {dataset}")
        files = self.list_files(dataset)

        if not files:
            raise FileNotFoundError(f"No files found for dataset '{dataset}' in s3://{self.bucket}/{self.prefix}")

        frames = [self.read_csv(f) for f in files]
        df     = pd.concat(frames, ignore_index=True)
        logger.info(f"  Loaded {len(df):,} rows from {len(files)} file(s)")

        df      = self.validate_schema(df, dataset)
        metrics = self.quality_check(df, dataset)

        logger.info(f"  Quality — nulls: {metrics['null_pct']}%, dupes: {metrics['duplicate_rows']}")
        return df, metrics


# ── Local fallback (for testing without AWS) ─────────────────────────────────
def extract_local(filepath: str, dataset: str) -> tuple[pd.DataFrame, dict]:
    """Read a local CSV instead of S3 (for dev/testing)."""
    extractor = S3Extractor(bucket="local", prefix="")
    df = pd.read_csv(filepath)
    df = extractor.validate_schema(df, dataset)
    metrics = extractor.quality_check(df, dataset)
    return df, metrics


if __name__ == "__main__":
    import os
    bucket = os.getenv("S3_BUCKET", "my-healthcare-bucket")
    ext    = S3Extractor(bucket=bucket)

    for ds in ["claims", "patients", "providers"]:
        df, metrics = ext.extract(ds)
        logger.info(f"✅ {ds}: {metrics}")
        df.to_parquet(f"/tmp/{ds}_raw.parquet", index=False)
