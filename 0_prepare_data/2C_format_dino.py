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

    # Compute median feature value per well
    meta_cols = [i for i in data.columns if "Metadata_" in i]
    feat_cols = [i for i in data.columns if "Metadata_" not in i]

    return data.group_by("Metadata_well").agg([
        pl.first(meta_cols).exclude("Metadata_image_id"),
        pl.median(feat_cols),
    ])


def main() -> None:
    """Format Dino embeddings.

    Merge Dino embeddings from each plate into one file and add metadata.

    """
    input_profile_path = "/Users/jewald/repos/2024_09_09_Axiom_OASIS/1_snakemake/inputs/profiles/dino/cpg0037-oasis/axiom/workspace/scratch"
    meta_path = "/Users/jewald/repos/2024_09_09_Axiom_OASIS/1_snakemake/inputs/metadata/metadata.parquet"
    output_profile_path = "/Users/jewald/repos/2024_09_09_Axiom_OASIS/1_snakemake/inputs/profiles/dino/raw.parquet"

    meta = pl.read_parquet(meta_path)

    batches = os.listdir(input_profile_path)
    profiles = []
    for batch in batches:
        batch_prof_path = f"{input_profile_path}/{batch}"
        batch_meta_path = f"{meta_path}/{batch}"

        plates = os.listdir(batch_prof_path)
        for plate in plates:
            meta_path = f"{batch_meta_path}/{plate}/metadata.parquet"
            prof_path = f"{batch_prof_path}/{plate}/dinov2_b_fieldnorm.parquet"
            profiles.append(process_dino_plate(prof_path, meta_path))

    data = pl.concat(profiles, how="vertical")
    data = data.join(meta, on=["Metadata_Plate", "Metadata_Well"])

    data.write_parquet(output_profile_path)


if __name__ == "__main__":
    main()
