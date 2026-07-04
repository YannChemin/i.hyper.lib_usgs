#!/usr/bin/env python
##############################################################################
# MODULE:    i.hyper.lib_usgs
# AUTHOR(S): Spectral Feature Extraction and Interpretation Engine
# PURPOSE:   Harvest the USGS Spectral Library Version 7 (splib07a) ASCII
#            data release into the same shared, source-tagged, partitioned
#            local spectral database used by i.hyper.lib_ecosis (GeoParquet
#            by default, SQLite fallback), usable by any i.hyper.* module.
# COPYRIGHT: (C) 2026 by the GRASS Development Team
# SPDX-License-Identifier: GPL-2.0-or-later
##############################################################################

# %module
# % description: Harvest the USGS Spectral Library Version 7 (splib07a) ASCII data release into the shared local spectral database used by i.hyper.* modules
# % keyword: imagery
# % keyword: hyperspectral
# % keyword: spectral library
# % keyword: USGS
# %end

# %option
# % key: input_dir
# % type: string
# % required: yes
# % description: Root directory of a local USGS Spectral Library Version 7 download (containing ASCIIdata/ and HTMLmetadata/ subdirectories, e.g. from https://doi.org/10.5066/F7RR1WDJ)
# % guisection: Input
# %end

# %option
# % key: output
# % type: string
# % required: no
# % description: Shared spectral library root directory (default: $HOME/grassdata/hyperspeclib -- the same fixed location used by i.hyper.lib_ecosis and future i.hyper.lib_* harvesters)
# % guisection: Output
# %end

# %option
# % key: format
# % type: string
# % required: no
# % options: parquet,sqlite
# % answer: parquet
# % description: Storage backend. parquet supports fast partitioned/parallel columnar access; sqlite is a zero-extra-dependency fallback
# % guisection: Output
# %end

# %option
# % key: chapters
# % type: string
# % required: no
# % multiple: yes
# % options: ChapterA_ArtificialMaterials,ChapterC_Coatings,ChapterL_Liquids,ChapterM_Minerals,ChapterO_OrganicCompounds,ChapterS_SoilsAndMixtures,ChapterV_Vegetation
# % description: Restrict ingestion to specific splib07a chapters (default: all)
# % guisection: Input
# %end

# %option
# % key: stream_batch_size
# % type: integer
# % required: no
# % answer: 2000
# % description: Number of records buffered in memory per incremental write batch (bounds peak memory regardless of chapter size)
# % guisection: Input
# %end

# %flag
# % key: f
# % description: Force re-ingestion of chapters already present in the library (default: skip chapters whose source file count hasn't grown since last ingestion)
# % guisection: Input
# %end

# %flag
# % key: k
# % description: Keep going on a per-sample parse error instead of aborting the whole run
# %end

from __future__ import annotations

import os
import re
import sys
import json
import sqlite3
import datetime
from typing import Optional

import grass.script as gs

SOURCE_DATABASE = "usgs_splib07"
DEFAULT_LIBRARY_ROOT = os.path.join(os.path.expanduser("~"), "grassdata", "hyperspeclib")
MANIFEST_NAME = "_manifest.json"

# The USGS DS1035 data release itself has no per-sample web page/API (unlike
# EcoSIS) -- these two links are the stable, citable online provenance for
# every record harvested by this module; per-record traceability instead
# comes from the local ASCII spectrum file + HTMLmetadata file paths kept in
# extra_metadata (see build_row()).
_CITATION_URL = "https://doi.org/10.3133/ds1035"
_DATA_RELEASE_URL = "https://dx.doi.org/10.5066/F7RR1WDJ"

_ALL_CHAPTERS = [
    "ChapterA_ArtificialMaterials",
    "ChapterC_Coatings",
    "ChapterL_Liquids",
    "ChapterM_Minerals",
    "ChapterO_OrganicCompounds",
    "ChapterS_SoilsAndMixtures",
    "ChapterV_Vegetation",
]

# Instrument families with native-resolution wavelength files in splib07a
# (splib07b and the sensor-convolved sublibraries -- AVIRIS-year, CRISM,
# Sentinel2, Landsat8, etc. -- are a different, much larger corpus and are
# out of scope for this harvester; splib07a is the original, native-
# sampling measured library).
_INSTRUMENT_FAMILIES = ("ASD", "BECK", "NIC4", "AVIRIS")

_BAD_VALUE_THRESHOLD = -1e30  # splib07a's "deleted channel" sentinel is -1.23e34

# ---------------------------------------------------------------------------
# splib07a ASCII data: sample spectra + shared wavelength/bandpass files
# ---------------------------------------------------------------------------


def _open_latin1(path: str):
    return open(path, encoding="iso-8859-1")


def _read_value_column(path: str) -> list[float]:
    """Shared format for both sample spectra and wavelength/bandpass files:
    one header line, then one float per line."""
    values = []
    with _open_latin1(path) as f:
        f.readline()  # header
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                values.append(float(line))
            except ValueError:
                continue
    return values


def _instrument_family(instrument_code: str) -> str:
    for fam in _INSTRUMENT_FAMILIES:
        if instrument_code.startswith(fam):
            return fam
    return instrument_code


def load_wavelength_tables(ascii_root: str) -> dict:
    """Parse splib07a's shared per-instrument wavelength files (microns ->
    nm) once, keyed by instrument family (ASD/BECK/NIC4/AVIRIS). Sample
    files reference these by instrument code embedded in their own header
    line (see _parse_sample_header) rather than each carrying its own
    wavelengths."""
    tables = {}
    for name in os.listdir(ascii_root):
        m = re.match(r"splib07a_Wavelengths_([A-Za-z0-9]+)_", name)
        if not m:
            continue
        family = m.group(1).upper()
        microns = _read_value_column(os.path.join(ascii_root, name))
        tables[family] = [v * 1000.0 for v in microns]  # -> nm
    return tables


def load_fwhm_tables(ascii_root: str) -> dict:
    """Best-effort FWHM/bandpass lookup, keyed by whatever instrument tag
    follows 'Bandpass_(FWHM)_' in the filename -- finer-grained than the
    wavelength tables for ASD (ASDFR/ASDHR/ASDNG each have distinct
    bandpass). Absence of a match is not fatal; FWHM is convenience
    metadata, not required to store or use a spectrum."""
    tables = {}
    for name in os.listdir(ascii_root):
        m = re.match(r"splib07a_Bandpass_\(FWHM\)_([A-Za-z0-9]+)", name)
        if not m:
            continue
        tag = m.group(1).upper()
        microns = _read_value_column(os.path.join(ascii_root, name))
        tables[tag] = [v * 1000.0 for v in microns]  # -> nm
    return tables


def _parse_sample_header(line: str) -> Optional[dict]:
    """Header format: 'splib07a Record=NNNN: <description>  <INSTRUMENT+
    purity> <MEASUREMENT_TYPE>', e.g.:
    ' splib07a Record=5932: Kaolinite CM5                 BECKb AREF'
    """
    elements = line.split()
    if len(elements) < 4 or "=" not in elements[1]:
        return None
    libname = elements[0]
    try:
        record = int(elements[1].split("=")[1].rstrip(":"))
    except ValueError:
        record = None
    measurement_type = elements[-1]
    instrument_purity = elements[-2]
    description = " ".join(elements[2:-2])
    m = re.match(r"^([A-Z0-9]+)([a-zA-Z]*)$", instrument_purity)
    instrument, purity = (m.group(1), m.group(2)) if m else (instrument_purity, "")
    return {
        "libname": libname,
        "record": record,
        "description": description,
        "instrument": instrument,
        "purity": purity,
        "measurement_type": measurement_type,
    }


def parse_sample_file(path: str, wavelength_tables: dict) -> Optional[tuple[dict, list, list]]:
    """Parse one splib07a sample spectrum file, aligning it against the
    matching instrument-family wavelength table and dropping deleted
    (-1.23e34) channels. Returns (header, wavelengths_nm, values) or None
    if the header is unreadable or no matching wavelength table aligns
    (both logged by the caller, not here, so it can honor -k)."""
    with _open_latin1(path) as f:
        header_line = f.readline()
        if not header_line.strip():
            return None
        header = _parse_sample_header(header_line.strip())
        if header is None:
            return None
        raw_values = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                raw_values.append(float(line))
            except ValueError:
                continue

    family = _instrument_family(header["instrument"])
    wavelengths_nm = wavelength_tables.get(family)
    if wavelengths_nm is None or len(wavelengths_nm) != len(raw_values):
        return header, [], []  # caller treats empty spectrum as unusable

    pairs = [(w, v) for w, v in zip(wavelengths_nm, raw_values) if v > _BAD_VALUE_THRESHOLD]
    pairs.sort(key=lambda p: p[0])
    return header, [p[0] for p in pairs], [p[1] for p in pairs]

# ---------------------------------------------------------------------------
# HTMLmetadata/<sample>.html -- generic KEYWORD: value extraction
# ---------------------------------------------------------------------------

_META_CHUNK_RE = re.compile(r"<[Pp]>")
_META_KV_RE = re.compile(r"^\s*([A-Z][A-Z0-9_]*):\s*(.*)", re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")


def parse_html_metadata(path: str) -> dict:
    """USGS's per-sample HTMLmetadata files use a consistent (if informal)
    '<p>KEYWORD: value' convention across every chapter (DOCUMENTATION_FORMAT
    differs -- MINERAL/PLANT/... -- but the KEYWORD: value structure is
    shared), so a single generic parser covers all chapters without a
    per-chapter field schema. The composition analysis table (a real HTML
    <TABLE>, not KEYWORD: value text) is intentionally not parsed here --
    the flat fields (mineral/plant name, formula, locality, sample ID) are
    the ones worth making queryable; the table remains in the original
    HTML for anyone who follows local_metadata_html."""
    try:
        with _open_latin1(path) as f:
            html = f.read()
    except OSError:
        return {}
    meta = {}
    for chunk in _META_CHUNK_RE.split(html):
        m = _META_KV_RE.match(chunk)
        if not m:
            continue
        key, val = m.group(1), m.group(2)
        val = _TAG_RE.sub(" ", val)
        val = re.sub(r"\s+", " ", val).strip()
        if val and val.upper() not in ("END", key.upper()):
            meta[key.lower()] = val
    return meta


def _chapter_title(chapter_id: str) -> str:
    name = chapter_id.split("_", 1)[1] if "_" in chapter_id else chapter_id
    return re.sub(r"(?<!^)(?=[A-Z])", " ", name).strip()

# ---------------------------------------------------------------------------
# Record -> unified i.hyper.lib_* row schema (same schema as i.hyper.lib_ecosis)
# ---------------------------------------------------------------------------


def build_row(chapter_id: str, sample_path: str, html_root: str,
              wavelength_tables: dict) -> Optional[dict]:
    parsed = parse_sample_file(sample_path, wavelength_tables)
    if parsed is None:
        return None
    header, wavelengths, values = parsed
    if not wavelengths:
        return None

    base = os.path.basename(sample_path)
    record_id = os.path.splitext(base)[0]
    html_name = base[len("splib07a_"):-4] + ".html" if base.startswith("splib07a_") else None
    html_path = os.path.join(html_root, html_name) if html_name else None
    html_meta = parse_html_metadata(html_path) if html_path and os.path.isfile(html_path) else {}

    extra = dict(html_meta)
    extra["measurement_subtype"] = header["measurement_type"]
    extra["instrument_code"] = header["instrument"] + header["purity"]
    extra["usgs_record_number"] = header["record"]
    extra["local_spectrum_file"] = sample_path
    if html_path and os.path.isfile(html_path):
        extra["local_metadata_html"] = html_path

    title = html_meta.get("title") or header["description"] or record_id

    return {
        "source_database": SOURCE_DATABASE,
        "dataset_id": chapter_id,
        "dataset_title": _chapter_title(chapter_id),
        "record_id": record_id,
        "organization": "U.S. Geological Survey",
        "source_url": _CITATION_URL,
        "source_api_url": _DATA_RELEASE_URL,
        "longitude": None,
        "latitude": None,
        "measurement_type": "reflectance",
        "wavelength_unit": "nm",
        "n_bands": len(wavelengths),
        "wavelengths": wavelengths,
        "values": values,
        "extra_metadata": json.dumps(extra, default=str),
        "ingest_date": datetime.datetime.now().isoformat(),
    }


def iter_row_batches(chapter_id: str, sample_paths: list[str], html_root: str,
                      wavelength_tables: dict, batch_size: int, keep_going: bool):
    """Yield lists of built rows, batch_size at a time -- mirrors
    i.hyper.lib_ecosis.iter_row_batches() so both harvesters feed the same
    incremental writer backends below."""
    batch = []
    for path in sample_paths:
        try:
            row = build_row(chapter_id, path, html_root, wavelength_tables)
        except Exception as exc:
            msg = f"Could not parse {path}: {exc}"
            if keep_going:
                gs.warning(msg)
                continue
            gs.fatal(msg)
            return
        if row is not None:
            batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# ---------------------------------------------------------------------------
# Manifest (per-chapter ingest bookkeeping, for incremental/skip-if-complete)
# ---------------------------------------------------------------------------


def load_manifest(root: str) -> dict:
    path = os.path.join(root, MANIFEST_NAME)
    if os.path.isfile(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_manifest(root: str, manifest: dict) -> None:
    path = os.path.join(root, MANIFEST_NAME)
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


def manifest_key(dataset_id: str) -> str:
    return f"{SOURCE_DATABASE}:{dataset_id}"

# ---------------------------------------------------------------------------
# Parquet (GeoParquet when geometry present) backend -- identical schema and
# writer strategy to i.hyper.lib_ecosis, so both sources land in the same
# partitioned tree and are queryable together.
# ---------------------------------------------------------------------------

_PARQUET_SCHEMA_FIELDS = [
    ("source_database", "string"),
    ("dataset_id", "string"),
    ("dataset_title", "string"),
    ("record_id", "string"),
    ("organization", "string"),
    ("source_url", "string"),
    ("source_api_url", "string"),
    ("longitude", "double"),
    ("latitude", "double"),
    ("measurement_type", "string"),
    ("wavelength_unit", "string"),
    ("n_bands", "int32"),
    ("wavelengths", "list<double>"),
    ("values", "list<double>"),
    ("extra_metadata", "string"),
    ("ingest_date", "string"),
]


def _require_pyarrow():
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        gs.fatal(
            "format=parquet requires the 'pyarrow' package "
            "(pip install pyarrow). Use format=sqlite for a "
            "zero-extra-dependency alternative."
        )


def _has_shapely() -> bool:
    try:
        import shapely  # noqa: F401
        return True
    except ImportError:
        return False


def _geoparquet_metadata() -> bytes:
    return json.dumps({
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": None,  # OGC:CRS84 (lon/lat WGS84), GeoParquet default
            }
        },
    }).encode()


def _rows_to_arrow_table(rows: list[dict], include_geometry: bool):
    """USGS lab samples have no geolocation (longitude/latitude are always
    null), but include_geometry may still be True when this run writes into
    a shared library alongside geolocated sources -- an all-null geometry
    column costs almost nothing in Parquet (efficient null-run encoding)
    and keeps every dataset_*.parquet file's schema identical, which
    ParquetWriter and cross-file pyarrow.dataset reads both require."""
    import pyarrow as pa

    columns = {name: [] for name, _ in _PARQUET_SCHEMA_FIELDS}
    for r in rows:
        for name, _ in _PARQUET_SCHEMA_FIELDS:
            columns[name].append(r.get(name))

    arrays = {
        "source_database": pa.array(columns["source_database"], type=pa.string()),
        "dataset_id": pa.array(columns["dataset_id"], type=pa.string()),
        "dataset_title": pa.array(columns["dataset_title"], type=pa.string()),
        "record_id": pa.array(columns["record_id"], type=pa.string()),
        "organization": pa.array(columns["organization"], type=pa.string()),
        "source_url": pa.array(columns["source_url"], type=pa.string()),
        "source_api_url": pa.array(columns["source_api_url"], type=pa.string()),
        "longitude": pa.array(columns["longitude"], type=pa.float64()),
        "latitude": pa.array(columns["latitude"], type=pa.float64()),
        "measurement_type": pa.array(columns["measurement_type"], type=pa.string()),
        "wavelength_unit": pa.array(columns["wavelength_unit"], type=pa.string()),
        "n_bands": pa.array(columns["n_bands"], type=pa.int32()),
        "wavelengths": pa.array(columns["wavelengths"], type=pa.list_(pa.float64())),
        "values": pa.array(columns["values"], type=pa.list_(pa.float64())),
        "extra_metadata": pa.array(columns["extra_metadata"], type=pa.string()),
        "ingest_date": pa.array(columns["ingest_date"], type=pa.string()),
    }

    if include_geometry:
        arrays["geometry"] = pa.array([None] * len(rows), type=pa.binary())

    return pa.table(arrays)


def write_parquet_dataset_streaming(root: str, dataset_id: str, batch_iter,
                                     include_geometry: bool) -> tuple[Optional[str], int]:
    """One Parquet file per chapter, under a Hive-style
    source_database=usgs_splib07/ partition -- readable together with
    i.hyper.lib_ecosis's source_database=ecosis/ partition by any consumer
    walking the shared library root."""
    import pyarrow.parquet as pq

    part_dir = os.path.join(root, f"source_database={SOURCE_DATABASE}")
    os.makedirs(part_dir, exist_ok=True)
    out_path = os.path.join(part_dir, f"dataset_{dataset_id}.parquet")

    writer = None
    n_total = 0
    try:
        for batch_rows in batch_iter:
            if not batch_rows:
                continue
            table = _rows_to_arrow_table(batch_rows, include_geometry)
            if writer is None:
                schema = table.schema
                if include_geometry:
                    schema = schema.with_metadata(
                        {**(schema.metadata or {}), b"geo": _geoparquet_metadata()})
                writer = pq.ParquetWriter(out_path, schema, compression="zstd")
            writer.write_table(table)
            n_total += len(batch_rows)
    finally:
        if writer is not None:
            writer.close()

    if n_total == 0:
        return None, 0
    return out_path, n_total

# ---------------------------------------------------------------------------
# SQLite backend (zero extra dependency) -- same table as i.hyper.lib_ecosis
# ---------------------------------------------------------------------------

_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS spectra (
    source_database TEXT NOT NULL,
    dataset_id      TEXT NOT NULL,
    dataset_title   TEXT,
    record_id       TEXT NOT NULL,
    organization    TEXT,
    source_url      TEXT,
    source_api_url  TEXT,
    longitude       REAL,
    latitude        REAL,
    measurement_type TEXT,
    wavelength_unit TEXT,
    n_bands         INTEGER,
    wavelengths     TEXT,
    values_json     TEXT,
    extra_metadata  TEXT,
    ingest_date     TEXT,
    PRIMARY KEY (source_database, dataset_id, record_id)
);
CREATE INDEX IF NOT EXISTS idx_spectra_dataset ON spectra (source_database, dataset_id);
CREATE INDEX IF NOT EXISTS idx_spectra_geo ON spectra (longitude, latitude);
"""

_SQLITE_INSERT = (
    "INSERT OR REPLACE INTO spectra VALUES "
    "(:source_database,:dataset_id,:dataset_title,:record_id,:organization,"
    ":source_url,:source_api_url,:longitude,:latitude,:measurement_type,"
    ":wavelength_unit,:n_bands,:wavelengths,:values_json,:extra_metadata,:ingest_date)"
)


def write_sqlite_dataset_streaming(root: str, batch_iter) -> tuple[Optional[str], int]:
    db_path = os.path.join(root, "hyperspeclib.sqlite")
    con = sqlite3.connect(db_path)
    n_total = 0
    try:
        con.executescript(_SQLITE_DDL)
        for batch_rows in batch_iter:
            if not batch_rows:
                continue
            con.executemany(_SQLITE_INSERT, [
                {**r, "wavelengths": json.dumps(r["wavelengths"]),
                 "values_json": json.dumps(r["values"])}
                for r in batch_rows
            ])
            con.commit()
            n_total += len(batch_rows)
    finally:
        con.close()

    if n_total == 0:
        return None, 0
    return db_path, n_total

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(options, flags):
    input_dir = options["input_dir"]
    output_root = options.get("output") or DEFAULT_LIBRARY_ROOT
    fmt = options.get("format", "parquet") or "parquet"
    chapters = [c for c in (options.get("chapters", "") or "").split(",") if c] or _ALL_CHAPTERS
    stream_batch_size = int(options.get("stream_batch_size", "2000") or "2000")
    force = flags["f"]
    keep_going = flags["k"]

    ascii_root = os.path.join(input_dir, "ASCIIdata", "ASCIIdata_splib07a")
    html_root = os.path.join(input_dir, "HTMLmetadata")
    if not os.path.isdir(ascii_root):
        gs.fatal(
            f"{ascii_root} not found -- input_dir should be the root of a "
            "USGS Spectral Library Version 7 download (containing "
            "ASCIIdata/ASCIIdata_splib07a/ and HTMLmetadata/)."
        )
    if not os.path.isdir(html_root):
        gs.warning(f"{html_root} not found -- proceeding without per-sample metadata enrichment.")

    if fmt == "parquet":
        _require_pyarrow()
    include_geometry = fmt == "parquet" and _has_shapely()

    gs.message("Loading splib07a instrument wavelength tables…")
    wavelength_tables = load_wavelength_tables(ascii_root)
    if not wavelength_tables:
        gs.fatal(f"No splib07a_Wavelengths_*.txt files found under {ascii_root}.")
    gs.verbose(f"Instrument families available: {sorted(wavelength_tables)}")

    os.makedirs(output_root, exist_ok=True)
    manifest = load_manifest(output_root)

    n_datasets_written = 0
    n_records_written = 0
    n_datasets_skipped = 0

    for i, chapter_id in enumerate(chapters):
        gs.percent(i, len(chapters), 2)
        chapter_dir = os.path.join(ascii_root, chapter_id)
        if not os.path.isdir(chapter_dir):
            gs.warning(f"Chapter directory not found, skipping: {chapter_dir}")
            continue

        sample_paths = sorted(
            os.path.join(chapter_dir, name)
            for name in os.listdir(chapter_dir)
            if name.startswith("splib07a_") and name.endswith(".txt")
        )
        expected = len(sample_paths)
        if expected == 0:
            continue

        key = manifest_key(chapter_id)
        prev_entry = manifest.get(key, {})
        prev_expected = prev_entry.get("expected_total", prev_entry.get("n_records", 0))
        if not force and prev_expected and prev_expected >= expected:
            gs.verbose(f"Skipping '{chapter_id}': already ingested "
                      f"({prev_entry.get('n_records', 0)} of {prev_expected} "
                      "records). Use -f to force re-ingestion.")
            n_datasets_skipped += 1
            continue

        batches = iter_row_batches(chapter_id, sample_paths, html_root,
                                    wavelength_tables, stream_batch_size, keep_going)
        if fmt == "parquet":
            out_path, n_rows = write_parquet_dataset_streaming(
                output_root, chapter_id, batches, include_geometry)
        else:
            out_path, n_rows = write_sqlite_dataset_streaming(output_root, batches)

        if n_rows == 0:
            gs.warning(f"Chapter '{chapter_id}' yielded no usable spectra; skipped.")
            continue

        manifest[key] = {
            "dataset_title": _chapter_title(chapter_id),
            "n_records": n_rows,
            "expected_total": expected,
            "last_updated": datetime.datetime.now().isoformat(),
            "path": out_path,
        }
        save_manifest(output_root, manifest)
        n_datasets_written += 1
        n_records_written += n_rows
        gs.verbose(f"Wrote {n_rows} record(s) for '{chapter_id}' → {out_path}")

    gs.percent(len(chapters), len(chapters), 2)

    gs.message(
        f"Done: {n_datasets_written} chapter(s) ingested ({n_records_written} spectra), "
        f"{n_datasets_skipped} already up to date, "
        f"library at {output_root} (format={fmt})."
    )


if __name__ == "__main__":
    options, flags = gs.parser()
    sys.exit(main(options, flags))
