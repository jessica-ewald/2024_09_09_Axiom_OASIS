import os

import polars as pl


def main() -> None:
    """Format CellProfiler profiles.

    Merge profiles from each plate into one file and add metadata.

    """
    input_index_dir = "../1_snakemake/inputs/images/load_data_csv"
    output_index_path = "../1_snakemake/inputs/images/index.parquet"

    plate_index = []
    plates = os.listdir(input_index_dir)
    plates = [i for i in plates if "plate_" in i]

    select_cols = [
        "Metadata_Plate",
        "Metadata_Well",
        "Metadata_Site",
        "PathName_OrigBrightfield",
        "FileName_OrigBrightfield",
        "FileName_OrigRNA",
        "FileName_OrigDNA",
        "FileName_OrigMito",
        "FileName_OrigER",
        "FileName_OrigAGP",
    ]

    # Read in data for each plate
    for plate in plates:
        index_path = f"{input_index_dir}/{plate}"
        plate_temp = pl.read_csv(index_path, infer_schema_length=10000).select(select_cols)
        plate_index.append(plate_temp)

    # Concat together
    index = pl.concat(plate_index, how="vertical_relaxed")

    index = index.with_columns(
        pl.when(pl.col("PathName_OrigBrightfield").str.contains("prod_25"))
        .then(pl.lit("prod_25"))
        .when(pl.col("PathName_OrigBrightfield").str.contains("prod_26"))
        .then(pl.lit("prod_26"))
        .when(pl.col("PathName_OrigBrightfield").str.contains("prod_27"))
        .then(pl.lit("prod_27"))
        .when(pl.col("PathName_OrigBrightfield").str.contains("prod_30"))
        .then(pl.lit("prod_30"))
        .alias("Metadata_Batch"),
    )

    index = index.rename({
        "FileName_OrigBrightfield": "Brightfield",
        "FileName_OrigRNA": "RNA",
        "FileName_OrigDNA": "DNA",
        "FileName_OrigMito": "Mito",
        "FileName_OrigER": "ER",
        "FileName_OrigAGP": "AGP",
    })

    index = index.melt(
        id_vars=["Metadata_Batch", "Metadata_Plate", "Metadata_Well", "Metadata_Site"],
        value_vars=["Brightfield", "RNA", "DNA", "Mito", "ER", "AGP"],
        variable_name="Channel",
        value_name="Filename",
    )

    index.write_parquet(output_index_path)


if __name__ == "__main__":
    main()
