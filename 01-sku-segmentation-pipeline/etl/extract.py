"""
extract.py — Extract Layer
DHL SKU Segmentation Pipeline — Project 01

Reads all source CSV files from the shared data directory.
Validates file existence, non-emptiness, and expected columns.
Logs file sizes and row counts.
Returns clean DataFrames ready for transformation.
"""

import os
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Resolve paths relative to this file's location
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR.parent.parent / "shared" / "data" / "dhl-synthetic"

# Expected source files and their required columns
EXPECTED_FILES = {
    "sku_master.csv": [
        "SKU_ID", "Category", "ABC_Class", "Unit_Cost", "Unit_Price",
        "Weight_KG", "Volume_CBM", "Storage_Type", "Supplier_ID",
        "Lead_Time_Days", "Min_Order_Qty", "Safety_Stock_Qty",
        "Reorder_Point_Qty", "Primary_Warehouse", "Active_Flag"
    ],
    "daily_demand.csv": [
        "Date", "SKU_ID", "Category", "Warehouse_ID", "ABC_Class",
        "XYZ_Class", "Quantity_Demanded", "Quantity_Fulfilled",
        "Stockout_Flag", "Revenue"
    ],
    "inventory_snapshot.csv": [
        "Snapshot_Date", "SKU_ID", "Warehouse_ID", "Category",
        "On_Hand_Qty", "In_Transit_Qty", "Committed_Qty",
        "Available_Qty", "Inventory_Value"
    ],
    "suppliers.csv": [
        "Supplier_ID", "Supplier_Name", "Country", "Category_Focus",
        "Lead_Time_Avg_Days", "Lead_Time_Std_Days", "OTIF_Rate",
        "Fill_Rate", "Defect_Rate", "Active_Flag"
    ],
    "warehouse_locations.csv": [
        "Location_ID", "Warehouse_ID", "Zone", "Aisle", "Bay",
        "Level", "Capacity_Units", "Storage_Type", "Active_Flag"
    ],
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "extract") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------

def validate_file(filepath: Path, required_cols: list, logger: logging.Logger) -> bool:
    """Validate a single source file exists, is non-empty, and has expected columns."""
    if not filepath.exists():
        logger.error(f"MISSING FILE: {filepath}")
        return False

    file_size_kb = filepath.stat().st_size / 1024
    if file_size_kb == 0:
        logger.error(f"EMPTY FILE: {filepath}")
        return False

    # Peek at header only
    header = pd.read_csv(filepath, nrows=0)
    missing_cols = [c for c in required_cols if c not in header.columns]
    if missing_cols:
        logger.error(f"MISSING COLUMNS in {filepath.name}: {missing_cols}")
        return False

    logger.info(f"  ✓ {filepath.name} — {file_size_kb:.1f} KB")
    return True


def extract_file(filepath: Path, logger: logging.Logger) -> pd.DataFrame:
    """Read a CSV file and log row count."""
    df = pd.read_csv(filepath, low_memory=False)
    logger.info(f"  Loaded {filepath.name}: {len(df):,} rows × {len(df.columns)} cols")
    return df


def extract_all(data_dir: Path = DATA_DIR, logger: logging.Logger = None) -> dict:
    """
    Main extract entry point.
    Returns dict of {table_name: DataFrame} for all source files.
    Raises RuntimeError if any file fails validation.
    """
    if logger is None:
        logger = get_logger("extract")

    logger.info("=" * 60)
    logger.info("EXTRACT STAGE — START")
    logger.info(f"Source directory: {data_dir}")
    logger.info("=" * 60)

    # --- Validate all files first ---
    logger.info("Validating source files...")
    errors = []
    for filename, cols in EXPECTED_FILES.items():
        fp = data_dir / filename
        if not validate_file(fp, cols, logger):
            errors.append(filename)

    if errors:
        raise RuntimeError(
            f"Extract validation failed for {len(errors)} file(s): {errors}"
        )
    logger.info(f"All {len(EXPECTED_FILES)} source files validated successfully.")

    # --- Load all files ---
    logger.info("Loading source files...")
    extracted = {}

    extracted["sku_master"] = extract_file(data_dir / "sku_master.csv", logger)
    extracted["daily_demand"] = extract_file(data_dir / "daily_demand.csv", logger)
    extracted["inventory_snapshot"] = extract_file(data_dir / "inventory_snapshot.csv", logger)
    extracted["suppliers"] = extract_file(data_dir / "suppliers.csv", logger)
    extracted["warehouse_locations"] = extract_file(data_dir / "warehouse_locations.csv", logger)

    # --- Summary ---
    total_rows = sum(len(df) for df in extracted.values())
    logger.info(f"Extract complete. Total rows loaded: {total_rows:,}")
    logger.info("EXTRACT STAGE — COMPLETE")

    return extracted


# ---------------------------------------------------------------------------
# Standalone run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger = get_logger("extract")
    data = extract_all(logger=logger)
    print("\nExtracted tables:")
    for name, df in data.items():
        print(f"  {name}: {len(df):,} rows")
