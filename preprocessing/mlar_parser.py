"""
preprocessing/mlar_parser.py
-----------------------------
Converts the MLAR Excel workbook (mlar-longrun-detailed.XLSX) into a single
long-format CSV file: mlar_long_raw.csv

Adapted from v1 — same parsing logic, same mapping files.
Changes from v2:
  - Output is one long-format CSV (src, category, quarter, value)
    instead of three wide CSVs
  - This simplifies the bronze table (fixed 4-column schema, no dynamic quarters)
  - Added parse_all() for the Airflow DAG
"""

import logging
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SHEET_CONFIGS = [
    {"sheet_name": "1.21", "header": 11, "footer": 7},
    {"sheet_name": "1.32", "header": 11, "footer": 8},
    {"sheet_name": "1.33", "header": 11, "footer": 9},
]


def _get_paths():
    try:
        from utils.config import MLAR_PATH, MLAR_XLSX_FILENAME
        xlsx_path = Path(MLAR_PATH) / MLAR_XLSX_FILENAME
        output_dir = Path(MLAR_PATH)
    except ImportError:
        xlsx_path = Path("data/mlar/mlar-longrun-detailed.XLSX")
        output_dir = Path("data/mlar")
    return xlsx_path, output_dir


def _get_mappings_dir() -> Path:
    """Mappings sit next to this script in preprocessing/mappings/."""
    return Path(__file__).parent / "mappings"


def preprocess(xlsx_path: Path, config: dict) -> pd.DataFrame:
    sheet_name = config["sheet_name"]

    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        engine="openpyxl",
        header=None,
        skiprows=config["header"],
        skipfooter=config["footer"],
    )

    # Build quarter column names from first two rows
    years = df.iloc[0].ffill()
    quarters = df.iloc[1]
    cols = [
        f"{int(y)}{q}" if isinstance(y, float) and not pd.isna(y) else f"{y}{q}"
        for y, q in zip(years, quarters)
    ]
    df.columns = cols
    df = df.iloc[2:]

    # Load mapping file
    mapping_file = _get_mappings_dir() / f"{sheet_name.replace('.', '_')}.csv"
    mapping_df = pd.read_csv(mapping_file)
    mapping = {}
    for _, row in mapping_df.iterrows():
        mapping[(row["section"], row["id"])] = row["label"]

    # Parse rows using section letters + numbered sub-rows
    new_rows = []
    current_section = None

    for _, row in df.iterrows():
        label = str(row.iloc[0]).strip()

        if label in ("A", "B", "C", "D", "E"):
            current_section = label
            continue

        if label.isdigit():
            num = int(label)
            key = (current_section, num)
            if key in mapping:
                category = mapping[key]
                new_row = {"category": category}
                for col in df.columns[4:]:
                    new_row[col] = row[col]
                new_rows.append(new_row)
            else:
                logger.warning("No mapping for key: %s", key)

    df_result = pd.DataFrame(new_rows)
    logger.info("Processed %d rows for worksheet %s", len(df_result), sheet_name)
    return df_result


def parse_all() -> str:
    """
    Parse all three MLAR sheets, transpose to long format,
    and save as one CSV. Called by the Airflow DAG.
    Returns the output file path.
    """
    xlsx_path, output_dir = _get_paths()

    if not xlsx_path.exists():
        raise FileNotFoundError(f"MLAR XLSX not found: {xlsx_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    all_long = []

    for config in SHEET_CONFIGS:
        df_wide = preprocess(xlsx_path, config)
        if df_wide is None or df_wide.empty:
            logger.warning("No data for sheet %s", config["sheet_name"])
            continue

        # Transpose: wide → long
        # Columns: category + quarter columns (2007Q1, 2007Q2, ...)
        quarter_cols = [c for c in df_wide.columns if c != "category"]
        df_long = df_wide.melt(
            id_vars=["category"],
            value_vars=quarter_cols,
            var_name="quarter",
            value_name="value",
        )
        df_long["src"] = config["sheet_name"]

        # Reorder to match bronze table: src, category, quarter, value
        df_long = df_long[["src", "category", "quarter", "value"]]

        all_long.append(df_long)
        logger.info(
            "Sheet %s: %d categories × %d quarters = %d rows",
            config["sheet_name"],
            len(df_wide),
            len(quarter_cols),
            len(df_long),
        )

    combined = pd.concat(all_long, ignore_index=True)
    output_path = output_dir / "mlar_long_raw.csv"
    combined.to_csv(output_path, index=False)
    logger.info("Saved: %s (%d total rows)", output_path, len(combined))
    return str(output_path)


if __name__ == "__main__":
    path = parse_all()
    logger.info("Done. Output: %s", path)