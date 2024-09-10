import os

import numpy as np
import pandas as pd
import polars as pl

from .metadata import find_feat_cols, find_meta_cols


def split_parquet(
    dframe_path: str,
    features=None,
) -> tuple[pd.DataFrame, np.ndarray, list[str]]:
    dframe = pd.read_parquet(dframe_path)
    if features is None:
        features = find_feat_cols(dframe)
    vals = np.empty((len(dframe), len(features)), dtype=np.float32)
    for i, c in enumerate(features):
        vals[:, i] = dframe[c]
    meta = dframe[find_meta_cols(dframe)].copy()
    return meta, vals, features


def load_data(input_path: str) -> pl.DataFrame:
    """Load all plates given the params."""
    b1_path = f"{input_path}/profiles/Batch1"
    b2_path = f"{input_path}/profiles/Batch2"

    b1_plate_path = [os.path.join(b1_path, file) for file in os.listdir(b1_path)]
    b2_plate_path = [os.path.join(b2_path, file) for file in os.listdir(b2_path)]

    plate_paths = b1_plate_path + b2_plate_path
    plates = os.listdir(b1_path) + os.listdir(b2_path)

    plate_paths = [i for i in plate_paths if "HepaRG" in i]
    plates = [i for i in plates if "HepaRG" in i]

    df_schema = pl.read_csv(
        f"{plate_paths[0]}/{plates[0]}.csv.gz",
        infer_schema_length=10000,
    )
    dat_schema = df_schema.schema

    dat_dfs = []
    for plate, path in zip(plates, plate_paths):
        csv_file = f"{path}/{plate}.csv.gz"

        dat = pl.read_csv(csv_file, schema=dat_schema, ignore_errors=True).drop([
            "Metadata_Site_Count",
            "Metadata_Count_CellsIncludingEdges",
            "Metadata_Count_Cytoplasm",
            "Metadata_Count_Nuclei",
            "Metadata_Count_NucleiIncludingEdges",
            "Metadata_Count_PreCellsIncludingEdges",
            "Metadata_Object_Count",
        ])
        dat_dfs.append(dat)

    dat_dfs = pl.concat(dat_dfs)
    return dat_dfs.rename({"Metadata_Well": "Metadata_rcWell"})


def load_metadata(input_path: str):
    """Load and process all metadata."""
    meta1_dir = f"{input_path}/metadata/platemaps/Batch1/platemap"
    meta2_dir = f"{input_path}/metadata/platemaps/Batch2/platemap"

    meta1_path = [os.path.join(meta1_dir, file) for file in os.listdir(meta1_dir)]
    meta2_path = [os.path.join(meta2_dir, file) for file in os.listdir(meta2_dir)]

    metapaths = meta1_path + meta2_path
    metapaths = [i for i in metapaths if "HepaRG" in i]

    schema_df = pl.read_csv(metapaths[0], separator="\t")
    meta_schema = schema_df.schema

    meta_dfs = []
    for metapath in metapaths:
        dat = pl.read_csv(metapath, schema=meta_schema, separator="\t")
        meta_dfs.append(dat)

    meta = pl.concat(meta_dfs)

    meta = meta.rename({
        "well_position": "Metadata_rcWell",
        "Source Well": "Metadata_SourceWell",
        "Destination Well": "Metadata_Well",
        "Sample_Type": "Metadata_SampleType",
        "Sample Name": "Metadata_Compound",
        "Test_Concentration": "Metadata_Concentration",
    }).drop([
        "plate_map_name",
        "Source Plate Name",
        "Destination Plate Name",
        "Transfer Volume",
        "Test_Concentration_Units",
        "Solvent",
    ])

    # Process metadata concentration column
    meta = meta.with_columns(
        pl.when(pl.col("Metadata_Concentration").is_null())
        .then(pl.lit("0"))
        .otherwise(pl.col("Metadata_Concentration"))
        .alias("Metadata_Concentration"),
    )

    meta = meta.with_columns(
        pl.when(pl.col("Metadata_Concentration") == "Not Dosed")
        .then(pl.lit("0"))
        .otherwise(pl.col("Metadata_Concentration"))
        .alias("Metadata_Concentration"),
    )

    meta = meta.with_columns(
        pl.col("Metadata_Concentration").cast(pl.Float64).alias("Metadata_Concentration"),
    )
    meta = meta.filter(
        ~((pl.col("Metadata_Compound") != "DMSO") & (pl.col("Metadata_Concentration") == 0)),
    )
    return meta.with_columns(
        pl.concat_str(
            ["Metadata_Compound", "Metadata_Concentration"],
            separator="_",
        ).alias("Metadata_Perturbation"),
    )


def merge_parquet(meta, vals, features, output_path: str) -> None:
    """Save the data in a parquet file resetting the index."""
    dframe = pd.DataFrame(vals, columns=features)
    for c in meta:
        dframe[c] = meta[c].reset_index(drop=True)
    dframe.to_parquet(output_path)


def write_parquet(input_path, next_lowest_dose, output_file):
    """Compile raw CP features and metadata from each plate into parquet."""
    dframe = load_data(input_path)
    meta = load_metadata(input_path)
    dframe = dframe.join(meta, on=["Metadata_Plate", "Metadata_rcWell"]).drop(
        "Metadata_rcWell",
    )

    # Add transformed concentrations
    log10_dose = dframe.select("Metadata_Concentration").to_series().to_numpy().copy()

    log10_dose[log10_dose == 0] = next_lowest_dose
    log10_dose = np.log10(log10_dose)
    log10_dose += abs(np.min(log10_dose))
    log10_dose = log10_dose.tolist()

    dframe = dframe.with_columns(
        pl.Series(name="Metadata_Log10Dose", values=log10_dose),
    )

    dframe.write_parquet(output_file)


def filter_cell_count(input_path: str, output_path: str) -> None:
    """Remove major cell count outliers from each group."""
    dat = pl.read_parquet(input_path)
    dat.write_parquet(output_path)

    # WILL WRITE ACTUAL FUNCTION HERE AFTER DOING EXPLORATORY ANALYSIS ON CORR DATA


def process_dino(input_path: str) -> pl.DataFrame:
    """Reformat Dino features.

    Returns
    -------
        pl.DataFrame: A single DataFrame with all the rows from the input DataFrames concatenated vertically.

    """
    channels = [
        "hoechst_33342",
        "phenovue_fluor_488",
        "wga555/phalloidin568",
        "cp_nuclei_acid_512",
        "mito_641_narrow",
        "brightfield",
    ]
    channel_nms = [
        "DNA",
        "ER",
        "AGP",
        "RNA",
        "Mito",
        "Brightfield",
    ]
    feat_names = []
    for nm in channel_nms:
        feat_names += [f"{nm}_{i:03}" for i in range(1, 769)]
    all_cols = ["Metadata_image_id", *feat_names]

    meta_keep = [
        "image_id",
        "source",
        "plate",
        "compound_concentration_um",
        "compound_name",
        "compound_scode",
        "compound_smiles",
        "compound_inchi",
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
    meta_nms = [f"Metadata_{i}" for i in meta_keep]

    plates = os.listdir(input_path)
    plate_dfs = []
    for plate in plates:
        # Process embeddings
        dat = pl.read_parquet(f"{input_path}/{plate}/dinov2_b_fieldnorm.parquet")
        data = []
        for i in range(dat.shape[0]):
            features = []
            for channel in channels:
                features += dat.select(channel)[i].item().to_list()
            data.append(features)

        data = pl.DataFrame(data)
        data.columns = feat_names

        data = data.with_columns(
            (pl.Series(name="Metadata_image_id", values=dat.select("image_id").to_series())),
        )
        data = data.select(all_cols)

        # Process metadata
        meta = pl.read_parquet(f"{input_path}/{plate}/metadata.parquet")
        meta = meta.select(meta_keep)
        meta.columns = meta_nms

        # Merge metadata and features together
        data_df = meta.join(data, on="Metadata_image_id")

        # Compute median feature value per well
        meta_cols = [i for i in data_df.columns if "Metadata_" in i]
        feat_cols = [i for i in data_df.columns if "Metadata_" not in i]

        data_median = data_df.group_by("Metadata_well").agg([
            pl.first(meta_cols).exclude("Metadata_image_id"),
            pl.median(feat_cols),
        ])

        plate_dfs.append(data_median)

    return pl.concat(plate_dfs, how="vertical")
