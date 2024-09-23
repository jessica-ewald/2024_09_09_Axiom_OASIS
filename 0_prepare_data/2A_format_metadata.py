import os

import polars as pl
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
    """Format metadata.

    Merge metadata from each plate into one file.

    """
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
