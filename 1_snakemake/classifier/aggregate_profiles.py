import logging

import numpy as np
import polars as pl
import pycytominer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def aggregate_compound(method: str, dat: pl.DataFrame) -> pl.DataFrame:
    """Aggregate subset of profiles for each compound"""

    feat_cols = [i for i in dat.columns if "Metadata" not in i]

    if method == "all":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.to_pandas(), strata=["Metadata_Compound"], features=feat_cols
            )
        )

    elif method == "allpod":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    pl.col("Metadata_Log10Dose") > pl.col("Metadata_POD")
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            )
        )

    elif method == "allpodcc":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    (pl.col("Metadata_Log10Dose") > pl.col("Metadata_POD"))
                    & (pl.col("Metadata_Log10Dose") < pl.col("Metadata_ccPOD"))
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            )
        )

    elif method == "lastpod":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    (pl.col("Metadata_Log10Dose") > pl.col("Metadata_POD"))
                    & (pl.col("Metadata_Concentration") == 50.0)
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            )
        )

    elif method == "firstpod":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    pl.col("Metadata_Concentration") == pl.col("Metadata_MinConc")
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            )
        )

    elif method == "lastpodcc":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    pl.col("Metadata_Concentration") == pl.col("Metadata_MaxConc")
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            )
        )

    # Annotate with aggregation and feature type
    agg_df = agg_df.with_columns(pl.lit(method).alias("Metadata_AggType"))

    return agg_df


def aggregate_profiles(
    prof_path: str, pod_path: str, rm_path: str, cpfeat_path: str, latent_path: str
) -> None:
    #### 1. Read in data
    profiles = pl.read_parquet(prof_path)
    pods = pl.read_parquet(pod_path)
    rot_mat = pl.read_parquet(rm_path).to_numpy()

    #### 2. Process metadata

    # remove controls
    controls = ["DMSO", "FCCP", "Berberine chloride"]
    profiles = profiles.filter(~pl.col("Metadata_Compound").is_in(controls))

    # Add POD
    profiles = profiles.join(
        pods.select(["Metadata_Compound", "bmd", "cc_POD"]).rename({
            "bmd": "Metadata_POD",
            "cc_POD": "Metadata_ccPOD",
        }),
        on="Metadata_Compound",
        how="left",
    )

    # Identify first conc after POD and last conc below ccPOD
    min_conc = (
        profiles.filter(pl.col("Metadata_Log10Dose") > pl.col("Metadata_POD"))
        .group_by(["Metadata_Compound"])
        .agg(pl.min("Metadata_Concentration").alias("Metadata_MinConc"))
    )

    max_conc = (
        profiles.filter(
            (pl.col("Metadata_Log10Dose") > pl.col("Metadata_POD"))
            & (pl.col("Metadata_Log10Dose") < pl.col("Metadata_ccPOD"))
        )
        .group_by(["Metadata_Compound"])
        .agg(pl.max("Metadata_Concentration").alias("Metadata_MaxConc"))
    )

    profiles = profiles.join(min_conc, on="Metadata_Compound", how="left").join(
        max_conc, on="Metadata_Compound", how="left"
    )

    #### 3. Create latent space profiles

    feat_cols = [col for col in profiles.columns if not col.startswith("Metadata_")]
    meta_cols = [col for col in profiles.columns if col.startswith("Metadata_")]

    latent_dat = profiles.select(feat_cols).to_numpy()
    latent_dat = np.dot(latent_dat, rot_mat)
    latent_dat = pl.DataFrame(latent_dat)

    new_column_names = [f"Comp{i + 1}" for i in range(len(latent_dat.columns))]
    latent_dat = latent_dat.rename({
        old: new for old, new in zip(latent_dat.columns, new_column_names)
    })

    latent_profiles = pl.concat(
        [profiles.select(meta_cols), latent_dat], how="horizontal"
    )

    #### 4. Aggregate profiles
    methods = ["all", "allpod", "allpodcc", "firstpod", "lastpod", "lastpodcc"]
    agg_df_cpfeat = []
    agg_df_latent = []

    for method in methods:
        agg_df_cpfeat.append(aggregate_compound(method, profiles))
        agg_df_latent.append(aggregate_compound(method, latent_profiles))

    agg_df_cpfeat = pl.concat(agg_df_cpfeat)
    agg_df_latent = pl.concat(agg_df_latent)

    #### 5. Write out results
    agg_df_cpfeat.write_parquet(cpfeat_path)
    agg_df_latent.write_parquet(latent_path)
