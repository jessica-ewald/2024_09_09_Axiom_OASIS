"""Download Axiom dino embeddings.

Use CPG index to download all Axiom Dino profiles.

"""  # noqa: CPY001, INP001

from pathlib import Path

import polars as pl
from cpgdata.utils import download_s3_files, parallel


def main() -> None:
    """Download data.

    Download index file, download data.

    """
    index_dir = Path("../1_snakemake/inputs/cpg_index")
    prof_dir = Path("../1_snakemake/inputs/profiles/dino")
    ncores = 10

    index_files = list(index_dir.glob("*.parquet"))

    # get Dino embedding paths
    index_df = pl.scan_parquet(index_files)
    index_df = (
        index_df.filter(pl.col("dataset_id").eq("cpg0037-oasis"))
        .filter(pl.col("leaf_node").str.contains("dinov2_b_fieldnorm.parquet"))
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
