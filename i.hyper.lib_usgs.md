# i.hyper.lib_usgs

## NAME

*i.hyper.lib_usgs* - Harvest the USGS Spectral Library Version 7 (splib07a)
ASCII data release into the same shared local spectral database used by
*i.hyper.lib_ecosis*, for use by *i.hyper.\** modules.

## SYNOPSIS

**i.hyper.lib_usgs**
**input_dir**=*string* [**output**=*string*] [**format**=*string*]
[**chapters**=*string*[,*string*,...]] [**stream_batch_size**=*value*]
[**-f**] [**-k**]

## DESCRIPTION

*i.hyper.lib_usgs* is the second *i.hyper.lib_\** harvester (after
*i.hyper.lib_ecosis*): it parses a local download of the USGS Spectral
Library Version 7 data release (Kokaly and others, 2017,
[https://doi.org/10.3133/ds1035](https://doi.org/10.3133/ds1035)) and
writes it into the **same shared, source-tagged database** at
`$HOME/grassdata/hyperspeclib`, using the exact same row schema as
*i.hyper.lib_ecosis* -- so a query spanning both harvesters' data (lab
mineral/vegetation/organic-compound reference spectra alongside
field-collected EcoSIS spectra) is a single `pyarrow.dataset` scan, no
per-source glue code needed.

Unlike EcoSIS, USGS's data release has no live query API -- it is a static
downloaded archive. **input_dir** must point at the root of that download
(e.g. `~/DBDATA/usgs_splib07`), containing its `ASCIIdata/` and
`HTMLmetadata/` subdirectories as distributed.

### Downloading the data

1. Go to the data release's ScienceBase landing page:
   [https://doi.org/10.5066/F7RR1WDJ](https://doi.org/10.5066/F7RR1WDJ)
   (Kokaly and others, 2017, *USGS Spectral Library Version 7 Data*, USGS
   data release). The companion report describing the library's contents
   and instruments is at
   [https://doi.org/10.3133/ds1035](https://doi.org/10.3133/ds1035)
   (Data Series 1035).
2. On the ScienceBase page, download the ASCII spectra archive (the file
   distributed as `ASCIIdata.zip` or similar, containing
   `ASCIIdata_splib07a/` -- the native-resolution measurements this
   harvester reads) and the HTML sample metadata archive (`HTMLmetadata.zip`).
   The page also offers `SPECPRsplib07/`, `GIFplots/`, and the oversampled
   `splib07b`/sensor-convolved sublibraries; none of those are needed by
   this harvester (see Notes).
3. Extract both archives into one common root directory, e.g.
   `~/DBDATA/usgs_splib07/`, so that it directly contains an `ASCIIdata/`
   subdirectory (with `ASCIIdata_splib07a/` inside it) and an
   `HTMLmetadata/` subdirectory as siblings -- this combined root is what
   **input_dir** must point at.
4. The full download is a few GB; only `ASCIIdata_splib07a/` (a few
   hundred MB) and `HTMLmetadata/` are actually read by this harvester,
   but both must be present at ingestion time since sample metadata is
   joined in from the latter per spectrum file.

### Why this needed its own parser (not shared with i.hyper.lib_ecosis)

USGS splib07a's on-disk layout is structurally different from EcoSIS's
`{dataset_info, spectra}` JSON, and was studied via the reference USGS/RELAB
parsers in `$HOME/dev/spectral/spectral/database/{usgs,relab}.py` (the
`spectral` Python package) for its parsing conventions -- not used as a
dependency, reimplemented here to fit this module's own schema and
streaming/batched-write architecture:

- **Spectra and wavelengths are in separate files.** Each sample is one
  `splib07a_<Name>_<Instrument><Purity>_<Type>.txt` file (e.g.
  `splib07a_Kaolinite_CM5_BECKb_AREF.txt`) under one of seven
  `Chapter*` subdirectories, holding only reflectance values, one per
  line, in the order defined by a *shared* per-instrument wavelength file
  (`splib07a_Wavelengths_<FAMILY>_...txt`, four native-resolution
  instrument families in splib07a: **ASD** (2151 channels, 0.35-2.5um),
  **BECK** (480 ch, 0.2-3.0um), **NIC4** (4595 ch, 1.12-216um), **AVIRIS**
  (224 ch, 0.37-2.5um)). A sample's own header line names its instrument
  (e.g. `BECKb`), which this module matches against the pre-parsed
  wavelength table for that family and aligns by position.
- **Deleted/bad channels** are marked `-1.23e34` in the value files (not
  simply absent) and are filtered out, same as EcoSIS's unparseable
  datapoints.
- **Sample header line** (`splib07a Record=5932: Kaolinite CM5 BECKb
  AREF`) is whitespace-tokenized (mirroring the reference parser): the
  record number, a free-text description, the instrument+purity code, and
  the measurement subtype (AREF/RREF/RTGC/...) are all packed into one
  line with no delimiters other than spaces, so field boundaries are
  inferred positionally (first two tokens, last two tokens, everything
  else is the description).
- **Rich per-sample metadata** lives in a *separate* `HTMLmetadata/<same
  base name>.html` file per sample, in an informal but consistent `<p>
  KEYWORD: value` convention across every chapter (`DOCUMENTATION_FORMAT`
  differs -- MINERAL, PLANT, MIXTURE, ... -- but the KEYWORD: value
  structure holds throughout), which a single generic parser extracts
  into `extra_metadata` without a per-chapter field schema: mineral/plant
  name, chemical formula, sample ID, collection locality, donor,
  spectral purity notes, etc. (The composition-analysis `<TABLE>` some
  mineral pages include is not parsed -- it's real tabular HTML, not
  KEYWORD: value text -- but remains reachable via the preserved
  `local_metadata_html` path.)

### Dataset grouping

USGS splib07a has no natural per-download "dataset" grouping the way
EcoSIS does; the seven `Chapter*_<Name>` subdirectories (Artificial
Materials, Coatings, Liquids, Minerals, Organic Compounds, Soils and
Mixtures, Vegetation) are the natural granularity instead, so each chapter
becomes one `dataset_id` / one Parquet file, exactly as one EcoSIS package
becomes one file. Every individual instrument measurement file is kept as
its own row (no attempt to pick one "canonical" instrument per physical
sample when several exist) -- complete and non-lossy, consistent with how
EcoSIS records are stored as submitted.

**chapters=** restricts ingestion to specific chapters (default: all
seven).

### No geolocation

USGS lab samples have no coordinates (a `COLLECTION_LOCALITY` free-text
field exists for some, e.g. "Lamar Pit, Bath S.C.", but it is not
structured lat/lon) -- `longitude`/`latitude` are always null for this
source, same nullable columns EcoSIS's non-georeferenced leaf-level
datasets already use. A shared library query does not need to special-case
this: a bounding-box filter simply matches zero USGS rows, an
attribute/text query over `dataset_title`/`extra_metadata` works
identically for both sources.

### No live per-record web page (unlike EcoSIS)

The DS1035 data release has no per-sample API or web page -- **source_url**
is the citation DOI (`https://doi.org/10.3133/ds1035`) and
**source_api_url** the ScienceBase data release landing page
(`https://dx.doi.org/10.5066/F7RR1WDJ`) for every row from this source.
Full per-record traceability instead comes from `extra_metadata`'s
`local_spectrum_file` and `local_metadata_html` paths, pointing back at
the exact files this run read.

### Incremental re-runs and streaming

Same conventions as *i.hyper.lib_ecosis*: a shared `_manifest.json` at the
database root (one entry per harvester+dataset, keyed
`usgs_splib07:<chapter>`) tracks each chapter's *source* file count
(`expected_total`) to decide whether a re-run can skip it (**-f** to
force), and records are batched (**stream_batch_size**, default 2000) and
written incrementally via the same `ParquetWriter`/`executemany()`
per-batch pattern -- though splib07a's individual files are tiny (a few KB
each, 2457 files total), so this matters far less here than for EcoSIS's
occasional multi-GB single file; it is kept for architectural consistency
between harvesters, not because it is load-bearing at this scale.

## NOTES

Use **-k** to keep going past a sample file that fails to parse or has no
matching wavelength table, instead of aborting the whole run.

Only the native-resolution **splib07a** measurements are ingested, not the
oversampled `splib07b` or the sensor-convolved sublibraries (`s07_ASD`,
`s07_AVxx`, CRISM, Sentinel2, Landsat8, WorldView3, ...) -- those are a
much larger, resampled-for-a-specific-sensor corpus, out of scope for this
harvester.

## EXAMPLES

### Ingest a single (small) chapter, SQLite backend

```sh
i.hyper.lib_usgs input_dir=$HOME/DBDATA/usgs_splib07 \
    output=/tmp/test_usgs_lib format=sqlite chapters=ChapterC_Coatings
```

```text
Loading splib07a instrument wavelength tables…
Instrument families available: ['ASD', 'AVIRIS', 'BECK', 'NIC4']
Wrote 12 record(s) for 'ChapterC_Coatings' →
/tmp/test_usgs_lib/hyperspeclib.sqlite
Done: 1 chapter(s) ingested (12 spectra), 0 already up to date, library at
/tmp/test_usgs_lib (format=sqlite).
```

One of those 12 rows, inspected directly:

```text
record_id:          splib07a_Blck_Mn_Coat_Tailngs_LV95-3_BECKb_AREF
dataset_title:      Coatings
n_bands:            453                 (of BECK's 480 -- 27 deleted channels dropped)
wavelengths[:3]:    [301.1, 305.1, 309.1]   (nm; BECK file is in microns, x1000)
extra_metadata:
  documentation_format: MIXTURE
  sample_id:            LV95-3
  mixture:              Mn-coating on rock
  collection_locality:  Leadville Mining District, Leadville, Colorado, USA
  original_donor:       Gregg Swayze
  instrument_code:      BECKb
  usgs_record_number:   13425
  local_spectrum_file:  .../ChapterC_Coatings/splib07a_Blck_Mn_Coat_Tailngs_LV95-3_BECKb_AREF.txt
  local_metadata_html:  .../HTMLmetadata/Blck_Mn_Coat_Tailngs_LV95-3_BECKb_AREF.html
```

### Full library: all seven chapters into the shared database

```sh
i.hyper.lib_usgs input_dir=$HOME/DBDATA/usgs_splib07
```

```text
Loading splib07a instrument wavelength tables…
Instrument families available: ['ASD', 'AVIRIS', 'BECK', 'NIC4']
Wrote 290 record(s) for 'ChapterA_ArtificialMaterials' → .../dataset_ChapterA_ArtificialMaterials.parquet
Wrote 12 record(s) for 'ChapterC_Coatings' → .../dataset_ChapterC_Coatings.parquet
Wrote 24 record(s) for 'ChapterL_Liquids' → .../dataset_ChapterL_Liquids.parquet
Wrote 1276 record(s) for 'ChapterM_Minerals' → .../dataset_ChapterM_Minerals.parquet
Wrote 360 record(s) for 'ChapterO_OrganicCompounds' → .../dataset_ChapterO_OrganicCompounds.parquet
Wrote 209 record(s) for 'ChapterS_SoilsAndMixtures' → .../dataset_ChapterS_SoilsAndMixtures.parquet
Wrote 286 record(s) for 'ChapterV_Vegetation' → .../dataset_ChapterV_Vegetation.parquet
Done: 7 chapter(s) ingested (2457 spectra), 0 already up to date, library
at /home/yann/grassdata/hyperspeclib (format=parquet).
```

Re-running the same command afterward is fully idempotent (`0 chapter(s)
ingested ... 7 already up to date`).

### Querying across both harvested sources at once

`i.hyper.lib_ecosis` had already populated the same library root with 195
EcoSIS datasets before this ran; both sources now live side by side under
their own `source_database=` partition, queryable together in one scan:

```python
import pyarrow.dataset as ds

dataset = ds.dataset("/home/yann/grassdata/hyperspeclib", format="parquet", partitioning="hive")
table = dataset.to_table(columns=["source_database"])
print(table.to_pandas()["source_database"].value_counts())
```

```text
source_database
ecosis          229893
usgs_splib07      2457
```

Finding every USGS record for a specific mineral, by filename convention
(a real per-source, per-name lookup a future spectral-matching
`i.hyper.*` module would do):

```python
table = dataset.to_table(
    columns=["record_id"],
    filter=(ds.field("source_database") == "usgs_splib07")
           & (ds.field("dataset_id") == "ChapterM_Minerals")
           & ds.field("record_id").utf8_startswith("splib07a_Kaolinite_CM5"),
)
# -> splib07a_Kaolinite_CM5_BECKb_AREF, splib07a_Kaolinite_CM5_NIC4bb_RREF
#    (two instrument measurements of the same physical sample, both kept)
```

## SEE ALSO

*[i.hyper.lib_ecosis](i.hyper.lib_ecosis.md),
[i.hyper.spectroscopy](i.hyper.spectroscopy.md),
[i.hyper.endmembers](i.hyper.endmembers.md)*

USGS Spectral Library Version 7: Kokaly, R.F., Clark, R.N., Swayze, G.A.,
Livo, K.E., Hoefen, T.M., Pearson, N.C., Wise, R.A., Benzel, W.M., Lowers,
H.A., Driscoll, R.L., and Klein, A.J., 2017, U.S. Geological Survey Data
Series 1035, 61 p., [https://doi.org/10.3133/ds1035](https://doi.org/10.3133/ds1035)

Data release: [https://doi.org/10.5066/F7RR1WDJ](https://doi.org/10.5066/F7RR1WDJ)

GeoParquet specification: [https://geoparquet.org](https://geoparquet.org)

## AUTHOR

Spectral Feature Extraction and Interpretation Engine
