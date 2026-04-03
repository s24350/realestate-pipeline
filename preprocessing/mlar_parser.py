"""
preprocessing/mlar_parser.py
-----------------------------
Converts the MLAR Excel workbook (mlar-longrun-detailed.XLSX) into three
flat CSV files: mlar_1_21.csv, mlar_1_32.csv, mlar_1_33.csv.

Adapted from v1 — same logic, same mapping files.
Changes: paths from config.py, added parse_all() for DAG.
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


def parse_all() -> list[str]:
    """Parse all three MLAR sheets and save as CSVs. Called by the DAG."""
    xlsx_path, output_dir = _get_paths()

    if not xlsx_path.exists():
        raise FileNotFoundError(f"MLAR XLSX not found: {xlsx_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_files = []

    for config in SHEET_CONFIGS:
        df_result = preprocess(xlsx_path, config)
        if df_result is not None and not df_result.empty:
            sheet_name = config["sheet_name"].replace(".", "_")
            output_path = output_dir / f"mlar_{sheet_name}.csv"
            df_result.to_csv(output_path, index=False)
            logger.info("Saved: %s (%d rows)", output_path, len(df_result))
            output_files.append(str(output_path))

    return output_files


if __name__ == "__main__":
    files = parse_all()
    logger.info("Done. Created %d file(s).", len(files))
