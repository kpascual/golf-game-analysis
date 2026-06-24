#!/usr/bin/env python
# coding: utf-8

"""
FlightScope Data Pipeline
Reads raw CSV exports from FlightScope, cleans and normalizes them,
and writes the result to a local SQLite database.
"""

# Standard library
import glob
import os
import re
import sqlite3

# Third-party
import numpy as np
import pandas as pd
import duckdb
from sqlalchemy import create_engine


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_NAME = "golf_data.db"
RAW_DATA_DIR = (
    "/Users/prodmachine/pCloud Drive/Hobbies - Fun/Golf/"
    "Data Analysis/FlightScope data/"
)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_direction(value) -> str | float:
    """Return 'R', 'L', or NaN from a directional string like '5.2R'."""
    if isinstance(value, str):
        if "R" in value:
            return "R"
        if "L" in value:
            return "L"
    return np.nan


def parse_magnitude(value) -> float:
    """Return the numeric magnitude from a directional string, ignoring R/L."""
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        cleaned = value.replace("R", "").replace("L", "").replace(" ", "")
        try:
            return float(cleaned)
        except ValueError:
            pass
    return np.nan


def parse_signed_value(value) -> float:
    """
    Return a signed float from a directional string.
    Right is positive, Left is negative.
    """
    magnitude = parse_magnitude(value)
    if isinstance(value, str) and "L" in value:
        return -magnitude
    return magnitude


def parse_low_point(value) -> float:
    """
    Convert a low-point string (e.g. "1′ 3.5″" or "2.5″") to total inches.
    A leading minus sign is preserved.
    """
    if not isinstance(value, str):
        return np.nan

    feet_and_inches = re.fullmatch(r"(-?)([.0-9]+)′ ([.0-9]+)″", value)
    inches_only     = re.fullmatch(r"(-?)([.0-9]+)″", value)

    if feet_and_inches:
        sign, feet, inches = feet_and_inches.groups()
        total = float(feet) * 12 + float(inches)
        return -total if sign == "-" else total

    if inches_only:
        sign, inches = inches_only.groups()
        return -float(inches) if sign == "-" else float(inches)

    return np.nan


def parse_session_date(filename: str) -> str:
    """Extract the date portion (YYYY-MM-DD) from a source filename."""
    return filename.replace(".csv", "")[:10]


def parse_session_index(filename: str) -> int:
    """
    Extract the session index from a filename.
    Returns 1 for a plain date filename, or the character after the date for
    filenames like '2024-01-15_2.csv'.
    """
    stem = filename.replace(".csv", "")
    if len(stem) == 10:
        return 1
    if len(stem) > 10:
        try:
            return int(stem[11])
        except (ValueError, IndexError):
            pass
    return 0


# ---------------------------------------------------------------------------
# Directional column expansion
# ---------------------------------------------------------------------------

DIRECTIONAL_COLUMNS = [
    "swing_h",
    "launch_h",
    "launch_v",
    "ftt",
    "ftp",
    "club_path",
    "spin_axis",
    "lateral",
    "curve_dist",
]


def expand_directional_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each directional column, add three derived columns:
      - <col>_direction  : 'R', 'L', or NaN
      - <col>_value      : unsigned magnitude as float
      - <col>_value_adj  : signed float (R positive, L negative)
    """
    df = df.copy()
    for col in DIRECTIONAL_COLUMNS:
        df[f"{col}_direction"] = df[col].apply(parse_direction)
        df[f"{col}_value"]     = df[col].apply(parse_magnitude)
        df[f"{col}_value_adj"] = df[col].apply(parse_signed_value)
    return df


# ---------------------------------------------------------------------------
# Column name normalisation
# ---------------------------------------------------------------------------

# Explicit renames that can't be handled by a blanket rule
COLUMN_RENAMES = {
    "Club (mph)":  "club_speed",
    "Ball (mph)":  "ball_speed",
    "Carry (yds)": "carry_distance",
    "Total (yds)": "total_distance",
    "Roll (yds)":  "roll_distance",
    "Height (ft)": "height_ft",
    "Time (s)":    "hang_time",
}

# Compiled regex to strip unit suffixes from column names, e.g. " (mph)", " (°)"
UNIT_SUFFIX_PATTERN = re.compile(r"\s*\((mph|yds|ft|ftin|in|rpm|s|°F|°|%|psi)\)")


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_RENAMES)
    df.columns = (
        df.columns
        .str.replace(UNIT_SUFFIX_PATTERN, "", regex=True)
        .str.strip()
        .str.replace(" ", "_")
        .str.lower()
    )
    return df


# ---------------------------------------------------------------------------
# Data type casting
# ---------------------------------------------------------------------------

NUMERIC_COLUMNS = {
    "shot":             "int",
    "ball_speed":       "float",
    "club_speed":       "float",
    "smash":            "float",
    "carry_distance":   "float",
    "total_distance":   "float",
    "roll_distance":    "float",
    "height_ft":        "float",
    "hang_time":        "float",
    "aoa":              "float",
    "spin_loft":        "float",
    "swing_v":          "float",
    "lateral_impact":   "float",
    "vertical_impact":  "float",
    "descentv":         "float",
    "spin":             "float",
    "dynamic_loft":     "float",
    "launch_v":         "float",
    "practice_date_index": "int",
}


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def load_raw_csvs(data_dir: str) -> pd.DataFrame:
    """Read all CSV files in *data_dir* and concatenate them into one DataFrame."""
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    frames = []
    for path in csv_files:
        df = pd.read_csv(path)
        df["source_file"] = os.path.basename(path)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def clean_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """Drop summary rows and replace placeholder dashes with NaN."""
    summary_rows = ["Avg", "Dev", "asdf_combined"]
    return (
        df
        .query("shot not in @summary_rows")
        .replace("-", np.nan)
    )


def add_session_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Derive practice date and within-day session index from the source filename."""
    df = df.copy()
    df["practice_date"]       = df["source_file"].apply(parse_session_date)
    df["practice_date_index"] = df["source_file"].apply(parse_session_index)
    return df


def clean_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Create final sanitized table to be used for analysis purposes"""
    COLUMNS_DROP = [
        'lateral',
        'swing_h',
        'launch_h',
        'launch_v',
        'ftt',
        'ftp',
        'club_path',
        'spin_axis',
        'curve_dist',
        'low_point',
        'mode',
        'location',
        'temperature',
        'humidity',
        'air_pressure',
        'altitude',
        'wind_relative_direction',
        'wind_speed',
        'source_file'
    ]
    COLUMNS_RENAME = {
        'shot': 'shot_index',
        'descentv': 'descent_v',
        'swing_h_value_adj': 'swing_h',
        'launch_h_value_adj': 'launch_h',
        'launch_v_value_adj': 'launch_v',
        'ftt_value_adj': 'ftt',
        'ftp_value_adj': 'ftp',
        'club_path_value_adj': 'club_path',
        'spin_axis_value_adj': 'spin_axis',
        'lateral_value_adj': 'side_distance',
        'curve_dist_value_adj': 'curve_distance',
        'low_point_adj': 'low_point'
    }

    df = df.drop(columns = COLUMNS_DROP)
    df = df.rename(columns = COLUMNS_RENAME)

    return df


def build_clean_dataset(data_dir: str) -> pd.DataFrame:
    """Full pipeline: load → normalise → clean → enrich → cast."""
    df = load_raw_csvs(data_dir)
    """Basic data cleansing"""
    df = normalize_column_names(df)
    df = clean_raw_data(df)
    """Save raw values to database"""
    save_to_database(df, "shot_source")

    df = add_session_metadata(df)
    df = expand_directional_columns(df)
    df = df.astype(NUMERIC_COLUMNS)
    df["low_point_adj"] = df["low_point"].apply(parse_low_point)

    df = clean_final_columns(df)
    
    """Save sanitized values to database"""
    save_to_database(df, "shots")
    print(f"Saved {len(df):,} shots to '{DB_NAME}'.")


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def save_to_database(df: pd.DataFrame, table_name: str, db_name: str = DB_NAME) -> None:
    """Write *df* to *table_name* in the local SQLite database."""
    engine = create_engine(f"sqlite:///{db_name}")
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"  → Wrote {len(df):,} rows to table '{table_name}'.")



# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    build_clean_dataset(RAW_DATA_DIR)
