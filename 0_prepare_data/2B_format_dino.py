import os

import polars as pl


def process_dino_plate(input_profile_path: str, input_meta_path: str) -> pl.DataFrame:
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

    # Process embeddings
    dat = pl.read_parquet(input_profile_path)
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
    meta = pl.read_parquet(input_meta_path)
    missing_cols = [i for i in meta_keep if i not in meta.columns]
    for mc in missing_cols:
        meta = meta.with_columns(
            pl.lit(None).alias(mc),
        )
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

    return data_median


def process_dino(input_profile_path: str, input_meta_path: str) -> pl.DataFrame:
    batches = os.listdir(input_profile_path)
    profiles = []
    for batch in batches:
        batch_prof_path = f"{input_profile_path}/{batch}"
        batch_meta_path = f"{input_meta_path}/{batch}"

        plates = os.listdir(batch_prof_path)
        for plate in plates:
            meta_path = f"{batch_meta_path}/{plate}/metadata.parquet"
            prof_path = f"{batch_prof_path}/{plate}/dinov2_b_fieldnorm.parquet"
            profiles.append(process_dino_plate(prof_path, meta_path))

    return pl.concat(profiles, how="vertical")
