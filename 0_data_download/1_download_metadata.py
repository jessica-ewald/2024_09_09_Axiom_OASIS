"""Download Axiom metadata.

Use CPG index to download all Axiom platemap and biochem metadata.

"""  # noqa: CPY001, INP001

import os
from pathlib import Path

import polars as pl
from cpgdata.utils import download_s3_files, parallel
from tqdm import tqdm

meta_keep = [
    "image_id",
    "source",
    "plate",
    "compound_concentration_um",
    "compound_name",
    "compound_scode",
    "compound_smiles",
    "compound_target",
    "compound_pathway",
    "compound_biological_activity",
    "well",
    "microscope",
    "mtt_lumi",
    "ldh_abs_signal",
    "ldh_abs_background",
    "ldh_abs",
    "mtt_normalized",
    "ldh_normalized",
]

batches = ["prod_25", "prod_26", "prod_27", "prod_30"]


def process_meta(input_meta_path: str, meta_nms: list) -> pl.DataFrame:
    """Process metadata.

    Select columns of interest and rename.



    """
    meta = pl.read_parquet(input_meta_path)
    missing_cols = [i for i in meta_keep if i not in meta.columns]
    for mc in missing_cols:
        meta = meta.with_columns(
            pl.lit(None).alias(mc),
        )
    meta = meta.select(meta_keep)
    meta.columns = meta_nms

    return meta


def main() -> None:
    """Download metadata.

    Read in index file, download data.

    """
    index_dir = Path("../1_snakemake/inputs/cpg_index")
    prof_dir = Path("../1_snakemake/inputs/metadata")
    ncores = 10

    index_files = list(index_dir.glob("*.parquet"))

    # get metadata paths (both biochem.parquet and metadata.parquet)
    index_df = pl.scan_parquet(index_files)
    index_df = (
        index_df.filter(pl.col("dataset_id").eq("cpg0037-oasis"))
        .filter(pl.col("obj_key").str.contains("scratch"))
        .filter(pl.col("obj_key").str.contains("biochem"))
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

    # Process metadata
    meta_nms = [f"Metadata_{i}" for i in meta_keep]
    meta_path = "../1_snakemake/inputs/metadata/cpg0037-oasis/axiom/workspace/scratch/"
    meta = []
    for batch in batches:
        batch_path = f"{meta_path}/{batch}"

        plates = os.listdir(batch_path)
        for plate in tqdm(plates):
            plate_path = f"{batch_path}/{plate}/biochem.parquet"
            meta.append(process_meta(plate_path, meta_nms))
    meta = pl.concat(meta, how="vertical_relaxed")
    meta = meta.rename({
        "Metadata_plate": "Metadata_Plate",
        "Metadata_well": "Metadata_Well",
    })
    meta.write_parquet("../1_snakemake/inputs/metadata/merged_metadata.parquet")


if __name__ == "__main__":
    main()
