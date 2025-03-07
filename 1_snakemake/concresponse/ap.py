from copairs import map

def process_compound(cmpd: String, profiles: pl.DataFrame, dmso_profiles: pl.DataFrame):
    """ Function to process each compound in parallel """
    cmpd_profs = profiles.filter(pl.col("Metadata_Compound") == cmpd)
    cmpd_plates = cmpd_profs.select(pl.col("Metadata_Plate")).to_series().unique().to_list()
    cmpd_dmso = dmso_profiles.filter(pl.col("Metadata_Plate").is_in(cmpd_plates))

    cmpd_profs = pl.concat([cmpd_profs, cmpd_dmso])

    # Get DMSO indices and randomly select 20
    indices = cmpd_profs["Metadata_Compound"].eq("DMSO").arg_true()
    indices = np.random.choice(indices, size=min(20, len(indices)), replace=False)

    cmpd_profs = cmpd_profs.to_pandas()
    cmpd_dmso_ap = []

    for index in indices:
        cmpd_df = cmpd_profs.copy()
        cmpd_df.loc[index, "Metadata_Compound"] = cmpd

        reference_col = "Metadata_reference_index"

        df_activity = assign_reference_index(
            cmpd_df,
            "Metadata_Compound == 'DMSO'",
            reference_col=reference_col,
            default_value=-1,
        )

        pos_sameby = ["Metadata_Compound", reference_col]
        pos_diffby = []
        neg_sameby = []  # used to be plate
        neg_diffby = ["Metadata_Compound", reference_col]

        metadata = df_activity.filter(regex="^Metadata")
        cmpd_feats = df_activity.filter(regex="^(?!Metadata)").values

        activity_ap = map.average_precision(
            metadata, cmpd_feats, pos_sameby, pos_diffby, neg_sameby, neg_diffby
        )

        cmpd_dmso_ap.append(activity_ap[
            (activity_ap["Metadata_orig_compound"] == "DMSO") & 
            (activity_ap["Metadata_Compound"] == cmpd)
        ])

    if cmpd_dmso_ap:
        return pd.concat(cmpd_dmso_ap)
    return None

def ap(input_path: str, output_paths: list, distances: list) -> None:
    print(input_path)
    print(output_paths)
    print(distances)

    path_dict = dict(zip(distances, paths))

    input_profiles = pl.read_parquet(input_path)
    feat_cols = [i for i in input_profiles.columns if "Metadata" not in i]

    if "ap" in distances:
        # get only columns of interest
        profiles = input_profiles.select(["Metadata_Compound", "Metadata_Plate", "Metadata_Well"] + feat_cols)
        profiles = profiles.with_columns(
            pl.col("Metadata_Compound").alias("Metadata_orig_compound")
        )
        dmso_profiles = profiles.filter(pl.col("Metadata_Compound") == "DMSO")

        compounds = profiles.select(pl.col("Metadata_Compound")).to_series().unique().to_list()
        compounds = [i for i in compounds if "DMSO" not in i]

        # Calculate average_precision for each dmso
        random.seed(10)
        with ProcessPoolExecutor(max_workers=80) as executor:
            results = list(tqdm(executor.map(process_compound, compounds, profiles, dmso_profiles), total=len(compounds)))

        # Filter out None results and concatenate
        all_dmso_ap = pd.concat([r for r in results if r is not None])

        # Calculate average_precision for all treatments
        reference_col = "Metadata_reference_index"

        df_activity = assign_reference_index(
            cmpd_df,
            "Metadata_Compound == 'DMSO'",
            reference_col=reference_col,
            default_value=-1,
        )

        pos_sameby = ["Metadata_Compound", reference_col]
        pos_diffby = []
        neg_sameby = []  # used to be plate
        neg_diffby = ["Metadata_Compound", reference_col]

        metadata = df_activity.filter(regex="^Metadata")
        cmpd_feats = df_activity.filter(regex="^(?!Metadata)").values

        activity_ap = map.average_precision(
            metadata, cmpd_feats, pos_sameby, pos_diffby, neg_sameby, neg_diffby
        )
        activity_ap = activity_ap.query("Metadata_broad_sample != 'DMSO'")

        all_ap = pl.DataFrame(pd.concat([activity_ap, all_dmso_ap]))

        all_ap.write_parquet(path_dict["ap"])

