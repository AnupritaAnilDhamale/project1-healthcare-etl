"""
transform.py — Business Logic Transformations
Healthcare ETL Pipeline | Anuprita Dhamale
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def transform_claims(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich claims data into FACT_CLAIMS."""
    logger.info("Transforming claims...")

    df = df.copy()

    # Drop exact duplicates
    df = df.drop_duplicates(subset=["claim_id"])

    # Null handling
    df["paid_amount"]    = df["paid_amount"].fillna(0.0)
    df["billed_amount"]  = df["billed_amount"].fillna(0.0)
    df["claim_status"]   = df["claim_status"].fillna("PENDING").str.upper().str.strip()
    df["diagnosis_code"] = df["diagnosis_code"].fillna("UNKNOWN").str.upper().str.strip()

    # Derived columns
    df["denial_flag"]          = (df["claim_status"] == "DENIED").astype(int)
    df["payment_ratio"]        = np.where(
        df["billed_amount"] > 0,
        (df["paid_amount"] / df["billed_amount"]).round(4),
        0.0
    )
    df["claim_month"]          = pd.to_datetime(df["claim_date"]).dt.to_period("M").astype(str)
    df["claim_year"]           = pd.to_datetime(df["claim_date"]).dt.year
    df["high_value_flag"]      = (df["billed_amount"] > 10_000).astype(int)
    df["etl_loaded_at"]        = datetime.utcnow()

    # Remove negative amounts (data quality)
    before = len(df)
    df = df[df["billed_amount"] >= 0]
    removed = before - len(df)
    if removed > 0:
        logger.warning(f"  Removed {removed} rows with negative billed_amount")

    logger.info(f"  Claims transformed: {len(df):,} rows")
    return df[[
        "claim_id", "patient_id", "provider_id", "claim_date", "claim_month",
        "claim_year", "diagnosis_code", "billed_amount", "paid_amount",
        "payment_ratio", "claim_status", "denial_flag", "high_value_flag", "etl_loaded_at"
    ]]


def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich patient data into DIM_PATIENT."""
    logger.info("Transforming patients...")

    df = df.copy()
    df = df.drop_duplicates(subset=["patient_id"])

    df["gender"]     = df["gender"].str.upper().str.strip().fillna("UNKNOWN")
    df["plan_type"]  = df["plan_type"].str.upper().str.strip().fillna("UNKNOWN")
    df["zip_code"]   = df["zip_code"].astype(str).str.zfill(5)
    df["dob"]        = pd.to_datetime(df["dob"], errors="coerce")
    df["age"]        = ((datetime.utcnow() - df["dob"]).dt.days / 365.25).round(0).astype("Int64")
    df["age_bucket"] = pd.cut(
        df["age"], bins=[0, 18, 35, 50, 65, 120],
        labels=["0-18", "19-35", "36-50", "51-65", "65+"],
        right=True
    ).astype(str)
    df["etl_loaded_at"] = datetime.utcnow()

    logger.info(f"  Patients transformed: {len(df):,} rows")
    return df[[
        "patient_id", "first_name", "last_name", "dob",
        "gender", "age", "age_bucket", "zip_code", "plan_type", "etl_loaded_at"
    ]]


def transform_providers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean provider data into DIM_PROVIDER."""
    logger.info("Transforming providers...")

    df = df.copy()
    df = df.drop_duplicates(subset=["provider_id"])

    df["specialty"]      = df["specialty"].str.title().str.strip().fillna("Unknown")
    df["state"]          = df["state"].str.upper().str.strip().fillna("XX")
    df["provider_name"]  = df["provider_name"].str.title().str.strip()
    df["etl_loaded_at"]  = datetime.utcnow()

    logger.info(f"  Providers transformed: {len(df):,} rows")
    return df[["provider_id", "provider_name", "specialty", "npi", "state", "etl_loaded_at"]]


def build_revenue_mart(claims: pd.DataFrame, patients: pd.DataFrame,
                        providers: pd.DataFrame) -> pd.DataFrame:
    """Join fact + dims to produce MART_REVENUE (Tableau-ready)."""
    logger.info("Building MART_REVENUE...")

    mart = (
        claims
        .merge(patients[["patient_id", "age_bucket", "plan_type", "gender"]],
               on="patient_id", how="left")
        .merge(providers[["provider_id", "provider_name", "specialty", "state"]],
               on="provider_id", how="left")
    )

    # Revenue summary columns
    mart["net_revenue"]           = mart["paid_amount"]
    mart["denial_loss"]           = mart["billed_amount"] - mart["paid_amount"]

    logger.info(f"  Revenue mart rows: {len(mart):,}")
    return mart


if __name__ == "__main__":
    # Test with local parquet files from extract step
    claims_raw   = pd.read_parquet("/tmp/claims_raw.parquet")
    patients_raw = pd.read_parquet("/tmp/patients_raw.parquet")
    providers_raw = pd.read_parquet("/tmp/providers_raw.parquet")

    claims    = transform_claims(claims_raw)
    patients  = transform_patients(patients_raw)
    providers = transform_providers(providers_raw)
    mart      = build_revenue_mart(claims, patients, providers)

    claims.to_parquet("/tmp/fact_claims.parquet", index=False)
    patients.to_parquet("/tmp/dim_patient.parquet", index=False)
    providers.to_parquet("/tmp/dim_provider.parquet", index=False)
    mart.to_parquet("/tmp/mart_revenue.parquet", index=False)

    logger.info("✅ All transforms complete.")
    logger.info(f"   FACT_CLAIMS:    {len(claims):,} rows")
    logger.info(f"   DIM_PATIENT:    {len(patients):,} rows")
    logger.info(f"   DIM_PROVIDER:   {len(providers):,} rows")
    logger.info(f"   MART_REVENUE:   {len(mart):,} rows")
