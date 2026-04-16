#!/usr/bin/env python3
"""Build a unified SQLite database from Excel and CSV sources.

This script imports:
- neu_data.xlsx
- Paper_Summary_Bacon.xlsx
- scientific_knowledge_graph_dataset_finished.csv
- thompson_data.csv

All rows are normalized into a single table: unified_records.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data_fusion.db"

EXCEL_SOURCES = [
	BASE_DIR / "neu_data.xlsx",
	BASE_DIR / "Paper_Summary_Bacon.xlsx",
]

CSV_SOURCES = [
	BASE_DIR / "scientific_knowledge_graph_dataset_finished.csv",
	BASE_DIR / "thompson_data.csv",
]


CANONICAL_COLUMNS = [
	"source_file",
	"source_sheet",
	"source_row_index",
	"doi",
	"title",
	"author",
	"publication_title",
	"publication_date",
	"url",
	"keywords",
	"abstract",
	"publisher",
	"field_of_study",
	"is_data_fusion",
	"classification_reason",
	"method_name",
	"method_key",
	"method_description",
	"u1",
	"u2",
	"u3",
	"data_name",
	"dataset_url",
	"data_type",
	"collection_method",
	"spatial_coverage",
	"temporal_coverage",
	"data_format",
	"license",
	"provenance",
	"uncertainty_type",
	"description",
]


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
	"""Normalize column names and trim string values."""
	out = df.copy()
	out.columns = [str(col).replace("\xa0", " ").strip() for col in out.columns]
	for col in out.columns:
		if pd.api.types.is_object_dtype(out[col]):
			out[col] = out[col].map(
				lambda value: value.strip() if isinstance(value, str) else value
			)
	return out


def drop_blank_rows(df: pd.DataFrame) -> pd.DataFrame:
	"""Drop rows that are entirely empty."""
	return df.dropna(axis=0, how="all").reset_index(drop=True)


def to_text(value: Any) -> str | None:
	if pd.isna(value):
		return None
	text = str(value).strip()
	return text if text else None


def load_excel_tables(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
	"""Load DOI, Fusion_Method, and Data sheets from an Excel source."""
	workbook = pd.ExcelFile(path)
	sheet_lookup = {
		str(name).replace("\xa0", " ").strip().lower(): name for name in workbook.sheet_names
	}

	doi_sheet = sheet_lookup.get("doi")
	method_sheet = sheet_lookup.get("fusion_method") or sheet_lookup.get("fusion_method ")
	data_sheet = sheet_lookup.get("data")

	if not doi_sheet or not method_sheet or not data_sheet:
		raise ValueError(f"{path.name}: required sheets DOI/Fusion_Method/Data were not found")

	papers = clean_dataframe(pd.read_excel(path, sheet_name=doi_sheet))
	methods = clean_dataframe(pd.read_excel(path, sheet_name=method_sheet))
	datasets = clean_dataframe(pd.read_excel(path, sheet_name=data_sheet))

	# The Data sheet's first column is actually DOI but may carry a DOI literal as the header.
	if "DOI" not in datasets.columns and len(datasets.columns) > 0:
		first_col = datasets.columns[0]
		if first_col != "Data Name":
			datasets = datasets.rename(columns={first_col: "DOI"})

	return drop_blank_rows(papers), drop_blank_rows(methods), drop_blank_rows(datasets)


def normalize_excel_source(path: Path) -> pd.DataFrame:
	"""Create canonical records from one Excel source by joining paper/method/data rows."""
	papers, methods, datasets = load_excel_tables(path)

	papers = papers.rename(
		columns={
			"DOI": "doi",
			"Title": "title",
			"Author": "author",
			"Publication Title": "publication_title",
			"PublicationDate": "publication_date",
			"Publication Date": "publication_date",
			"URL": "url",
			"Keywords": "keywords",
			"Abstract": "abstract",
			"Publisher": "publisher",
			"Field of Study": "field_of_study",
			"IsDataFusionPaper": "is_data_fusion",
			"Is Data Fusion Paper": "is_data_fusion",
			"DataFusionClassificationReason": "classification_reason",
			"Data Fusion Classification Reason": "classification_reason",
		}
	)
	papers["field_of_study"] = papers.get("field_of_study", papers.get("Field of Study "))

	methods = methods.rename(
		columns={
			"Method Name": "method_name",
			"Method Key": "method_key",
			"DOI": "doi",
			"Description": "method_description",
			"U1": "u1",
			"U1 ": "u1",
			"U3": "u3",
		}
	)

	datasets = datasets.rename(
		columns={
			"DOI": "doi",
			"Data Name": "data_name",
			"DatasetURL": "dataset_url",
			"Method Key": "method_key",
			"Data Type": "data_type",
			"Collection Method": "collection_method",
			"U2": "u2",
			"SpatialCoverage": "spatial_coverage",
			"TemporalCoverage": "temporal_coverage",
			"Format": "data_format",
			"License": "license",
			"Provenance": "provenance",
		}
	)

	papers = papers[[col for col in papers.columns if col in set(CANONICAL_COLUMNS + ["doi"])]]
	methods = methods[[col for col in methods.columns if col in set(CANONICAL_COLUMNS + ["doi", "method_key"])]]
	datasets = datasets[[col for col in datasets.columns if col in set(CANONICAL_COLUMNS + ["doi", "method_key"])]]

	merged = datasets.merge(
		methods,
		on=["doi", "method_key"],
		how="left",
		suffixes=("", "_method"),
	).merge(
		papers,
		on="doi",
		how="left",
		suffixes=("", "_paper"),
	)

	merged["source_file"] = path.name
	merged["source_sheet"] = "Data+Fusion_Method+DOI"
	merged["source_row_index"] = merged.index + 1
	merged["description"] = merged.get("method_description")
	merged["uncertainty_type"] = None

	for col in CANONICAL_COLUMNS:
		if col not in merged.columns:
			merged[col] = None

	return merged[CANONICAL_COLUMNS]


def normalize_csv_source(path: Path) -> pd.DataFrame:
	"""Create canonical records from a summary CSV source."""
	frame = clean_dataframe(pd.read_csv(path))
	frame = drop_blank_rows(frame)

	frame = frame.rename(
		columns={
			"Paper Title": "title",
			"Dataset Name": "data_name",
			"Method Name": "method_name",
			"Uncertainty Type": "uncertainty_type",
			"Uncertainty Type (U1/U2/U3)": "uncertainty_type",
			"Description": "description",
		}
	)

	frame["source_file"] = path.name
	frame["source_sheet"] = "csv"
	frame["source_row_index"] = frame.index + 1

	for col in CANONICAL_COLUMNS:
		if col not in frame.columns:
			frame[col] = None

	return frame[CANONICAL_COLUMNS]


def create_schema(conn: sqlite3.Connection) -> None:
	conn.execute(
		"""
		CREATE TABLE IF NOT EXISTS unified_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_file TEXT,
			source_sheet TEXT,
			source_row_index INTEGER,
			doi TEXT,
			title TEXT,
			author TEXT,
			publication_title TEXT,
			publication_date TEXT,
			url TEXT,
			keywords TEXT,
			abstract TEXT,
			publisher TEXT,
			field_of_study TEXT,
			is_data_fusion TEXT,
			classification_reason TEXT,
			method_name TEXT,
			method_key TEXT,
			method_description TEXT,
			u1 TEXT,
			u2 TEXT,
			u3 TEXT,
			data_name TEXT,
			dataset_url TEXT,
			data_type TEXT,
			collection_method TEXT,
			spatial_coverage TEXT,
			temporal_coverage TEXT,
			data_format TEXT,
			license TEXT,
			provenance TEXT,
			uncertainty_type TEXT,
			description TEXT
		)
		"""
	)
	conn.execute("CREATE INDEX IF NOT EXISTS idx_unified_doi ON unified_records (doi)")
	conn.execute("CREATE INDEX IF NOT EXISTS idx_unified_method_key ON unified_records (method_key)")
	conn.execute("CREATE INDEX IF NOT EXISTS idx_unified_source_file ON unified_records (source_file)")


def build_database() -> None:
	frames: list[pd.DataFrame] = []

	for src in EXCEL_SOURCES:
		if not src.exists():
			raise FileNotFoundError(f"Missing source file: {src}")
		frames.append(normalize_excel_source(src))

	for src in CSV_SOURCES:
		if not src.exists():
			raise FileNotFoundError(f"Missing source file: {src}")
		frames.append(normalize_csv_source(src))

	unified = pd.concat(frames, ignore_index=True)
	unified = unified.apply(lambda col: col.map(to_text))

	with sqlite3.connect(DB_PATH) as conn:
		create_schema(conn)
		conn.execute("DELETE FROM unified_records")
		unified.to_sql("unified_records", conn, if_exists="append", index=False)
		conn.commit()

		total = conn.execute("SELECT COUNT(*) FROM unified_records").fetchone()[0]
		by_source = conn.execute(
			"SELECT source_file, COUNT(*) FROM unified_records GROUP BY source_file ORDER BY source_file"
		).fetchall()

	print(f"Created {DB_PATH.name}")
	print(f"Total unified rows: {total}")
	for source_file, count in by_source:
		print(f"  - {source_file}: {count}")


if __name__ == "__main__":
	build_database()
