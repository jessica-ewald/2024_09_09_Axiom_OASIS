"""Download Axiom metadata.

Use CPG index to download all Axiom platemap and biochem metadata.

"""  # noqa: CPY001, INP001

from pathlib import Path

import polars as pl
from cpgdata.utils import download_s3_files, parallel


def main() -> None:
    """Download metadata.

    Read in index file, download data.

    """
    index_dir = Path("../1_snakemake/inputs/cpg_index")
    prof_dir = Path("../1_snakemake/inputs/metadata/dino")
    ncores = 10

    index_files = list(index_dir.glob("*.parquet"))

    # get metadata paths (both biochem.parquet and metadata.parquet)
    index_df = pl.scan_parquet(index_files)
    index_df = (
        index_df.filter(pl.col("dataset_id").eq("cpg0037-oasis"))
        .filter(pl.col("obj_key").str.contains("scratch"))
        .filter(pl.col("obj_key").str.contains("metadata"))
        .select("key")
        .collect()
    )

    # Download files
    download_keys = list(index_df.to_dict()["key"])
    parallel(
        download_keys,
        download_s3_files,
        ["cellpainting-gallery", prof_dir],
        jobs=ncores,
    )


if __name__ == "__main__":
    main()
